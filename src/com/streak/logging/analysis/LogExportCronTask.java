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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.api.client.googleapis.extensions.appengine.auth.oauth2.AppIdentityCredential;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.log.LogService.LogLevel;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskAlreadyExistsException;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Builder;
import com.google.appengine.api.taskqueue.TaskOptions.Method;

public class LogExportCronTask extends HttpServlet {
	private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	private static final Logger logger = Logger.getLogger(LogExportCronTask.class.getName());

	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		resp.setContentType("text/plain");
		
		String msPerTableStr = req.getParameter(AnalysisConstants.MS_PER_TABLE_PARAM);
		long msPerTable = 1000 * 60 * 60 * 24;
		if (AnalysisUtility.areParametersValid(msPerTableStr)) {
			msPerTable = Long.parseLong(msPerTableStr);
		}
		
		String msPerFileStr = req.getParameter(AnalysisConstants.MS_PER_FILE_PARAM);
		long msPerFile = 1000 * 60 * 2;
		if (AnalysisUtility.areParametersValid(msPerFileStr)) {
			msPerFile = Long.parseLong(msPerFileStr);
		}

		if (msPerTable % msPerFile != 0) {
			throw new InvalidTaskParameterException("The " + AnalysisConstants.MS_PER_FILE_PARAM + " parameter must divide the " + AnalysisConstants.MS_PER_TABLE_PARAM + " parameter.");
		}
		
		String endMsStr = req.getParameter(AnalysisConstants.END_MS_PARAM);
		long endMs = System.currentTimeMillis();
		if (AnalysisUtility.areParametersValid(endMsStr)) {
			endMs = Long.parseLong(endMsStr);
		}
		String lookBackStr = req.getParameter(AnalysisConstants.LOOKBACK_PARAM);
		int lookBack = 10;
		if (AnalysisUtility.areParametersValid(lookBackStr)) {
			lookBack = Integer.parseInt(lookBackStr);
		}

		// By default look back a ways, but safely under the limit of 1000 files
		// per listing that Cloud Storage imposes
		String startMsStr = req.getParameter(AnalysisConstants.START_MS_PARAM);
		// For testing
		long startMs = endMs - msPerFile * lookBack;
		if (AnalysisUtility.areParametersValid(startMsStr)) {
			startMs = Long.parseLong(startMsStr);
		}
		String logVersion = AnalysisUtility.extractParameter(req, AnalysisConstants.LOG_VERSION);
		String logLevel = req.getParameter(AnalysisConstants.LOG_LEVEL_PARAM);
		if (!AnalysisUtility.areParametersValid(logLevel)) {
			logLevel = getDefaultLogLevel();
		}

		String deleteFromCloudStorage = req.getParameter(AnalysisConstants.DELETE_FROM_CLOUD_STORAGE_PARAM);
		
		// Verify that log level is one of the enum values or ALL
		if (!"ALL".equals(logLevel)) {
			LogLevel.valueOf(logLevel);
		}

		String bucketName = req.getParameter(AnalysisConstants.BUCKET_NAME_PARAM);
		if (!AnalysisUtility.areParametersValid(bucketName)) {
			bucketName = getDefaultBucketName();
		}
		
		String bigqueryProjectId = req.getParameter(AnalysisConstants.BIGQUERY_PROJECT_ID_PARAM);	
		if (!AnalysisUtility.areParametersValid(bigqueryProjectId)) {
			bigqueryProjectId = getDefaultBigqueryProjectId();
		}
		
		String bigqueryDatasetId = req.getParameter(AnalysisConstants.BIGQUERY_DATASET_ID_PARAM);
		if (!AnalysisUtility.areParametersValid(bigqueryDatasetId)) {
			bigqueryDatasetId = getDefaultBigqueryDatasetId();
		}
		
		String bigqueryFieldExporterSet = req.getParameter(AnalysisConstants.BIGQUERY_FIELD_EXPORTER_SET_PARAM);
		if (!AnalysisUtility.areParametersValid(bigqueryFieldExporterSet)) {
			bigqueryFieldExporterSet = getDefaultBigqueryFieldExporterSet();
		}
		// Instantiate the exporter set to detect errors before we spawn a bunch
		// of tasks.
		BigqueryFieldExporterSet exporterSet = 
				AnalysisUtility.instantiateExporterSet(bigqueryFieldExporterSet);
		String schemaHash = AnalysisUtility.computeSchemaHash(exporterSet);
		
		String queueName = req.getParameter(AnalysisConstants.QUEUE_NAME_PARAM);
		if (!AnalysisUtility.areParametersValid(queueName)) {
			queueName = getDefaultQueueName();
		}
		String maxErrorRecords = req.getParameter(AnalysisConstants.MAX_ERRORS);

		String useSystemTaskName = req.getParameter(AnalysisConstants.UNIQUE_TASK_NAME); // back door to repeat task
		AnalysisConstants.EnumSourceFormat format = exporterSet.getFormat();

		AppIdentityCredential credential = new AppIdentityCredential(AnalysisConstants.SCOPES);
		HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory(credential);
		
		List<String> urisToProcess = new ArrayList<String>();
		AnalysisUtility.fetchCloudStorageLogUris(
				bucketName, schemaHash, startMs, endMs, requestFactory, urisToProcess, true);
		long lastEndMsSeen = startMs - startMs % msPerFile;
		for (String uri : urisToProcess) {
			long uriEndMs = AnalysisUtility.getEndMsFromKey(uri);
			if (uriEndMs > lastEndMsSeen) {
				lastEndMsSeen = uriEndMs;
			}
		}
		
		List<String> fieldNames = new ArrayList<String>();
		List<String> fieldTypes = new ArrayList<String>();
		List<String> fieldModes = new ArrayList<String>();
		List<String> fieldFields = new ArrayList<String>();
		AnalysisUtility.populateSchema(exporterSet, fieldNames, fieldTypes, fieldModes, fieldFields);

		FileService fileService = FileServiceFactory.getFileService();
		
		Queue taskQueue = QueueFactory.getQueue(queueName);
		
		int taskCount = 0;
		for (long currentStartMs = lastEndMsSeen; currentStartMs + msPerFile <= endMs; currentStartMs += msPerFile) {
			long tableStartMs = currentStartMs - currentStartMs % msPerTable;
			long tableEndMs = tableStartMs + msPerTable;
			String tableName = AnalysisUtility.createLogKey(schemaHash, tableStartMs, tableEndMs);
			
			String schemaKey = AnalysisUtility.createSchemaKey(schemaHash, currentStartMs, currentStartMs + msPerFile);
			if (format == AnalysisConstants.EnumSourceFormat.JSON) {
				AnalysisUtility.writeJsonSchema(fileService, bucketName, schemaKey, fieldNames, fieldTypes, fieldModes, fieldFields);
			} else {
				AnalysisUtility.writeSchema(fileService, bucketName, schemaKey, fieldNames, fieldTypes);
			}

			TaskOptions taskOptions = Builder
					.withUrl(AnalysisUtility.getRequestBaseName(req) + "/storeLogsInCloudStorage")
					.method(Method.GET)
					.param(AnalysisConstants.START_MS_PARAM, "" + currentStartMs)
					.param(AnalysisConstants.END_MS_PARAM, "" + (currentStartMs + msPerFile))
					.param(AnalysisConstants.BUCKET_NAME_PARAM, bucketName)
					.param(AnalysisConstants.BIGQUERY_PROJECT_ID_PARAM, bigqueryProjectId)
					.param(AnalysisConstants.BIGQUERY_DATASET_ID_PARAM, bigqueryDatasetId)
					.param(AnalysisConstants.BIGQUERY_FIELD_EXPORTER_SET_PARAM, bigqueryFieldExporterSet)
					.param(AnalysisConstants.QUEUE_NAME_PARAM, queueName)
					.param(AnalysisConstants.BIGQUERY_TABLE_ID_PARAM, tableName)
					.param(AnalysisConstants.LOG_LEVEL_PARAM, logLevel);

			if (AnalysisUtility.areParametersValid(deleteFromCloudStorage)) {
				taskOptions.param(AnalysisConstants.DELETE_FROM_CLOUD_STORAGE_PARAM, deleteFromCloudStorage);
			}

			String taskNameStr = "csstore_" + bucketName + "_" + currentStartMs;
			if (!AnalysisUtility.areParametersValid(useSystemTaskName)) {
				// set unique task name to prevent duplicates / idempotency for BQ import
				taskOptions.taskName(taskNameStr);
				logger.info("Task name: " + taskNameStr);
			} else {
				taskOptions.param(AnalysisConstants.UNIQUE_TASK_NAME, useSystemTaskName);
			}

			if (logVersion != null && !logVersion.isEmpty()) {
				taskOptions.param(AnalysisConstants.LOG_VERSION, logVersion);
			}
			if (maxErrorRecords != null && !maxErrorRecords.isEmpty()) {
				taskOptions.param(AnalysisConstants.MAX_ERRORS, maxErrorRecords);
			}

			try {
				taskQueue.add(taskOptions);
				taskCount += 1;
			} catch (TaskAlreadyExistsException e) {
				// just error log to prevent task restarts
				logger.warning("Error creating task '" + taskNameStr + "' (tombstoned): " + e.getMessage());
			}
		}
		resp.getWriter().println("Successfully started " + taskCount + " tasks");
		logger.info("Successfully started " + taskCount + " tasks");
	}
	
	protected String getDefaultBucketName() {
		return "logs";
	}
	
	protected String getDefaultBigqueryProjectId() {
		return "42541920816";
	}
	
	protected String getDefaultBigqueryDatasetId() {
		return "logsdataset";
	}
	
	protected String getDefaultBigqueryFieldExporterSet() {
		return "com.streak.logging.analysis.example.BasicFieldExporterSet";
	}
	
	protected String getDefaultQueueName() {
		return QueueFactory.getDefaultQueue().getQueueName();
	}
	
	protected String getDefaultLogLevel() {
		return "ALL";
	}
}
