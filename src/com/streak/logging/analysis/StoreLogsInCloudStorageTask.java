/*
 * Copyright 2012 Rewardly Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streak.logging.analysis;

import java.io.IOException;
import java.text.Normalizer.Form;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.api.log.LogQuery;
import com.google.appengine.api.log.LogService;
import com.google.appengine.api.log.LogService.LogLevel;
import com.google.appengine.api.log.LogServiceFactory;
import com.google.appengine.api.log.RequestLogs;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskAlreadyExistsException;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Builder;
import com.google.appengine.api.taskqueue.TaskOptions.Method;
import com.streak.logging.analysis.AnalysisConstants.EnumSourceFormat;

public class StoreLogsInCloudStorageTask extends HttpServlet {
	private static final Logger logger = Logger
			.getLogger(StoreLogsInCloudStorageTask.class.getName());

	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		resp.setContentType("text/plain");

		String startMsStr = AnalysisUtility.extractParameterOrThrow(req,
				AnalysisConstants.START_MS_PARAM);
		long startMs = Long.parseLong(startMsStr);

		String endMsStr = AnalysisUtility.extractParameterOrThrow(req,
				AnalysisConstants.END_MS_PARAM);
		long endMs = Long.parseLong(endMsStr);

		String bucketName = AnalysisUtility.extractParameterOrThrow(req,
				AnalysisConstants.BUCKET_NAME_PARAM);
		String queueName = AnalysisUtility.extractParameterOrThrow(req,
				AnalysisConstants.QUEUE_NAME_PARAM);
		String logLevelStr = AnalysisUtility.extractParameterOrThrow(req,
				AnalysisConstants.LOG_LEVEL_PARAM);
		LogLevel logLevel = null;
		if (!"ALL".equals(logLevelStr)) {
			logLevel = LogLevel.valueOf(logLevelStr);
		}
		// Idempotency by spletart (if taskName query param set, then do not set
		// taskName in taskOptions)
		String taskNameStr = null;
		if (!AnalysisUtility.areParametersValid(req
				.getParameter(AnalysisConstants.UNIQUE_TASK_NAME))) {
			String tableName = req
					.getParameter(AnalysisConstants.BIGQUERY_TABLE_ID_PARAM);
			// set uniqueId to prevent duplicates / idempotency for BQ import
			taskNameStr = this.getClass().getSimpleName() + "_" + tableName
					+ "_" + startMs;
		}
		String formatStr = req.getParameter(AnalysisConstants.SCHEMA_FORMAT);
		EnumSourceFormat format = EnumSourceFormat.CSV;
		try {
			format = EnumSourceFormat.valueOf(formatStr);
		} catch (Exception e) {
		}

		String logVersion = AnalysisUtility.extractParameter(req,
				AnalysisConstants.LOG_VERSION);
		String exporterSetClassStr = AnalysisUtility.extractParameterOrThrow(
				req, AnalysisConstants.BIGQUERY_FIELD_EXPORTER_SET_PARAM);
		BigqueryFieldExporterSet exporterSet = AnalysisUtility
				.instantiateExporterSet(exporterSetClassStr);
		String schemaHash = AnalysisUtility.computeSchemaHash(exporterSet);

		List<String> fieldNames = new ArrayList<String>();
		List<String> fieldTypes = new ArrayList<String>();
		List<String> fieldModes = new ArrayList<String>();
		List<String> fieldFields = new ArrayList<String>();

		AnalysisUtility.populateSchema(exporterSet, fieldNames, fieldTypes,
				fieldModes, fieldFields);

		String respStr = generateExportables(startMs, endMs, bucketName,
				schemaHash, exporterSet, fieldNames, fieldTypes, logLevel,
				logVersion, format);
		Queue taskQueue = QueueFactory.getQueue(queueName);

		TaskOptions taskOptions = Builder
				.withUrl(
						AnalysisUtility.getRequestBaseName(req)
								+ "/loadCloudStorageToBigquery?"
								+ req.getQueryString()).method(Method.GET);
		// set unique task name to prevent duplicates / idempotency for BQ
		// import
		if (null != taskNameStr) {
			taskOptions.taskName(taskNameStr);
		}
		try {
			taskQueue.add(taskOptions);
		} catch (TaskAlreadyExistsException e) {
			// just error log to prevent task restarts
			logger.warning("Error creating task (tombstoned): "
					+ e.getMessage());
		}
		resp.getWriter().println(respStr);
	}

	protected String generateExportables(long startMs, long endMs,
			String bucketName, String schemaHash,
			BigqueryFieldExporterSet exporterSet, List<String> fieldNames,
			List<String> fieldTypes, LogLevel logLevel, String version,
			EnumSourceFormat format) throws IOException {
		List<BigqueryFieldExporter> exporters = exporterSet.getExporters();

		LogService ls = LogServiceFactory.getLogService();
		LogQuery lq = new LogQuery();
		lq = lq.startTimeUsec(startMs * 1000).endTimeUsec(endMs * 1000)
				.includeAppLogs(true);

		if (logLevel != null) {
			lq = lq.minLogLevel(logLevel);
		}
		if (version != null && !version.isEmpty()) {
			lq.majorVersionIds(Arrays.asList(version));
		}

		String fileKey = AnalysisUtility.createLogKey(schemaHash, startMs,
				endMs);

		FancyFileWriter writer = new FancyFileWriter(bucketName, fileKey);
		Iterable<RequestLogs> logs = ls.fetch(lq);

		int resultsCount = 0;
		for (RequestLogs log : logs) {
			// filter logs
			if (exporterSet.skipLog(log)) {
				continue;
			}

			// write record...
			if (format == EnumSourceFormat.JSON) {
				resultsCount += writeJsonRecords(exporterSet, exporters, log,
						writer, fieldNames, fieldTypes);
			} else {
				resultsCount += writeCsvRecords(exporterSet, exporters, log,
						writer, fieldNames, fieldTypes);
			}

			// just ping fileWriter to handle possible timeouts
			writer.append("");
		}
		writer.closeFinally();
		String msg = "Saved " + resultsCount + " logs to gs://" + bucketName
				+ "/" + fileKey;
		logger.info(msg);
		return msg;
	}

	protected int writeCsvRecords(BigqueryFieldExporterSet exporterSet,
			List<BigqueryFieldExporter> exporters, RequestLogs log,
			FancyFileWriter writer, List<String> fieldNames,
			List<String> fieldTypes) throws IOException {

		int resultsCount = 0;
		int rows = exporterSet.getRecordsCount(log);
		for (int i = 0; i < rows; i++) {
			int exporterStartOffset = 0;
			int currentOffset = 0;
			for (BigqueryFieldExporter exporter : exporters) {
				exporter.processLog(log);
				while (currentOffset < exporterStartOffset
						+ exporter.getFieldCount()) {
					if (currentOffset > 0) {
						writer.append(",");
					}
					Object fieldValue = exporter.getField(
							fieldNames.get(currentOffset), i);
					if (fieldValue == null) {
						// Just skip this one...
						// throw new InvalidFieldException(
						// "Exporter " + exporter.getClass().getCanonicalName()
						// +
						// " didn't return field for " +
						// fieldNames.get(currentOffset));
						// logger.warning("Exporter " +
						// exporter.getClass().getCanonicalName() +
						// " didn't return field for " +
						// fieldNames.get(currentOffset));
					}

					writer.append(AnalysisUtility.formatCsvValue(fieldValue,
							fieldTypes.get(currentOffset)));
					currentOffset++;
				}
				exporterStartOffset += exporter.getFieldCount();
			}
			writer.append("\n");

			resultsCount++;
		}

		return resultsCount++;
	}

	private int writeJsonRecords(BigqueryFieldExporterSet exporterSet,
			List<BigqueryFieldExporter> exporters, RequestLogs log,
			FancyFileWriter writer, List<String> fieldNames,
			List<String> fieldTypes) throws IOException {

		int resultsCount = 0;
		int rows = exporterSet.getRecordsCount(log);
		for (int row = 0; row < rows; row++) {

			int exporterStartOffset = 0;
			int currentOffset = 0;

			writer.append("{");
			for (BigqueryFieldExporter exporter : exporters) {
				exporter.processLog(log);
				while (currentOffset < exporterStartOffset
						+ exporter.getFieldCount()) {
					if (currentOffset > 0) {
						writer.append(",");
					}
					String fieldName = exporter.getFieldName(currentOffset);
					Object fieldValue = exporter.getField(
							fieldNames.get(currentOffset), row);

					writer.append("\"" + fieldName + "\":");
					writer.append(AnalysisUtility.formatCsvValue(fieldValue,
							fieldTypes.get(currentOffset)));
					currentOffset++;
				}
				exporterStartOffset += exporter.getFieldCount();
			}
			writer.append("}\n");
			resultsCount++;
		}

		return resultsCount++;
	}

}
