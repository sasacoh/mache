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

import com.google.api.client.googleapis.extensions.appengine.auth.oauth2.AppIdentityCredential;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.*;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions.Builder;
import com.google.appengine.api.taskqueue.TaskOptions.Method;
import com.streak.logging.analysis.AnalysisConstants.EnumSourceFormat;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class LoadCloudStorageToBigqueryTask extends HttpServlet {
	private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static final Logger logger = Logger.getLogger(LoadCloudStorageToBigqueryTask.class.getName());

	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		resp.setContentType("text/plain");

		String queueName = AnalysisUtility.extractParameterOrThrow(req, AnalysisConstants.QUEUE_NAME_PARAM);
		String bigqueryProjectId = AnalysisUtility.extractParameterOrThrow(req, AnalysisConstants.BIGQUERY_PROJECT_ID_PARAM);
		String bigqueryDatasetId = AnalysisUtility.extractParameterOrThrow(req, AnalysisConstants.BIGQUERY_DATASET_ID_PARAM);
		String bigqueryTableId = AnalysisUtility.extractParameterOrThrow(req, AnalysisConstants.BIGQUERY_TABLE_ID_PARAM);
// Do not use rate limiting...
//		MemcacheService memcache = MemcacheServiceFactory.getMemcacheService(AnalysisConstants.MEMCACHE_NAMESPACE);
//		Long nextBigQueryJobTime =
//				(Long) memcache.increment(
//						AnalysisConstants.LAST_BIGQUERY_JOB_TIME, AnalysisConstants.LOAD_DELAY_MS, System.currentTimeMillis());
//
//		long currentTime = System.currentTimeMillis();
//
//		// The task queue has waited a long time to run us. Go ahead and reset the last job time
//		// to prevent a race.
//		if (currentTime > nextBigQueryJobTime + AnalysisConstants.LOAD_DELAY_MS / 2) {
//			memcache.put(AnalysisConstants.LAST_BIGQUERY_JOB_TIME, currentTime);
//			nextBigQueryJobTime = currentTime + AnalysisConstants.LOAD_DELAY_MS;
//		}
//		if (currentTime < nextBigQueryJobTime) {
//			memcache.increment(AnalysisConstants.LAST_BIGQUERY_JOB_TIME, -AnalysisConstants.LOAD_DELAY_MS);
//			Queue taskQueue = QueueFactory.getQueue(queueName);
//			taskQueue.add(
//					Builder.withUrl(
//							AnalysisUtility.getRequestBaseName(req) +
//									"/loadCloudStorageToBigquery?" + req.getQueryString())
//							.method(Method.GET)
//							.etaMillis(nextBigQueryJobTime));
//
//			logger.info("Rate limiting BigQuery load job - will retry at " + nextBigQueryJobTime);
//			resp.getWriter().println("Rate limiting BigQuery load job - will retry at " + nextBigQueryJobTime);
//			return;
//		}

		String bucketName = AnalysisUtility.extractParameterOrThrow(req, AnalysisConstants.BUCKET_NAME_PARAM);

		AppIdentityCredential credential = new AppIdentityCredential(AnalysisConstants.SCOPES);
		HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory(credential);

		List<String> urisToProcess = new ArrayList<String>();

		String schemaBaseUri;
		String startMsStr = req.getParameter(AnalysisConstants.START_MS_PARAM);
		// Logs
		if (AnalysisUtility.areParametersValid(startMsStr)) {
			long startMs = Long.parseLong(startMsStr);

			String endMsStr = AnalysisUtility.extractParameterOrThrow(req, AnalysisConstants.END_MS_PARAM);
			long endMs = Long.parseLong(endMsStr);
			String exporterSetClassStr = AnalysisUtility.extractParameterOrThrow(req, AnalysisConstants.BIGQUERY_FIELD_EXPORTER_SET_PARAM);
			BigqueryFieldExporterSet exporterSet = AnalysisUtility.instantiateExporterSet(exporterSetClassStr);
			String schemaHash = AnalysisUtility.computeSchemaHash(exporterSet);
			AnalysisUtility.fetchCloudStorageLogUris(
					bucketName, schemaHash, startMs, endMs, requestFactory, urisToProcess, false);

			if (urisToProcess.isEmpty()) {
				// By Spletart: Could be here - why don't retry!!!
				int count = 0;
				long currentTime = System.currentTimeMillis();
				Long nextBigQueryJobTime = currentTime + AnalysisConstants.LOAD_DELAY_MS;
                String queryString = req.getQueryString();
				String retry = req.getParameter("retry");
				if (retry != null && !retry.isEmpty()) {
					try {
						count = Integer.parseInt(retry);
						if (count >= 3) {
							// OK, I give up now...
							logger.warning("No uris to process from fetchCloudStorageUris. Retry limit hit '" + count + "'. Giving up.");
							return;
						}
					} catch (NumberFormatException e) {
						logger.warning("No uris to process from fetchCloudStorageUris. 'NumberFormatException' Giving up.");
						return;
					}
                    queryString = queryString.replaceAll("retry=\\d", "retry=" + (count + 1));
				}
                else {
                    queryString += "&retry=" + (count + 1);
                }
				// retrying...
				logger.warning("No uris to process from fetchCloudStorageUris. Retry count: '" + count + "' - will retry at " + nextBigQueryJobTime);
                Queue taskQueue = QueueFactory.getQueue(queueName);
				taskQueue.add(
						Builder.withUrl(
								AnalysisUtility.getRequestBaseName(req) +
										"/loadCloudStorageToBigquery?" + queryString)
								.method(Method.GET)
								.etaMillis(nextBigQueryJobTime));
				resp.getWriter().println("No uris to process from fetchCloudStorageUris - will retry at " + nextBigQueryJobTime);

				return;
			}

			schemaBaseUri = urisToProcess.get(0);
			// Datastore
		} else {
			String cloudStoragePathBase = req.getParameter(AnalysisConstants.CLOUD_STORAGE_PATH_BASE_PARAM);
			String cloudStoragePathBaseEnd = cloudStoragePathBase.substring(0, cloudStoragePathBase.length() - 1) + (char) (cloudStoragePathBase.charAt(cloudStoragePathBase.length() - 1) + 1);
			AnalysisUtility.fetchCloudStorageUris(bucketName, cloudStoragePathBase, cloudStoragePathBaseEnd, requestFactory, urisToProcess, false);
			schemaBaseUri = "gs://" + bucketName + "/" + cloudStoragePathBase;
		}
		resp.getWriter().println("Got " + urisToProcess.size() + " uris to process");

		if (urisToProcess.isEmpty()) {
			return;
		}
		// Import job idempotency by spletart
		String jobId = null;
		// not forced uniqueness by client (back door) and startMs is not null or empty
		if (!AnalysisUtility.areParametersValid(req.getParameter(AnalysisConstants.UNIQUE_TASK_NAME)) && null != startMsStr && !startMsStr.isEmpty()) {
			// set managed uniqueId to prevent duplicates / idempotency for BQ import
			jobId = "job_" + bigqueryDatasetId + "_" + bigqueryTableId + "_" + startMsStr;
		}
		String maxErrorsStr = req.getParameter(AnalysisConstants.MAX_ERRORS);
		// experimental default
		int maxErrors = 5;
		if (maxErrorsStr != null && !maxErrorsStr.isEmpty()) {
			try {
				maxErrors = Integer.parseInt(maxErrorsStr);
			} catch (Exception e) {
			}
		}
		// Set source format...
		String formatStr = req.getParameter(AnalysisConstants.SCHEMA_FORMAT);
		EnumSourceFormat format = EnumSourceFormat.CSV;
		try {
			format = EnumSourceFormat.valueOf(formatStr);
		} catch (Exception e) {
		}

		for (String uri : urisToProcess) {
			resp.getWriter().println("URI: " + uri);
		}

		Bigquery bigquery = new Bigquery.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
				.setApplicationName("Streak Logs")
				.build();

		Job job = new Job();
		JobConfiguration config = new JobConfiguration();
		JobConfigurationLoad loadConfig = new JobConfigurationLoad();

		loadConfig.setSourceUris(urisToProcess);
		loadConfig.set("allowQuotedNewlines", true);

		TableSchema schema = new TableSchema();
		if (format == EnumSourceFormat.JSON) {
			AnalysisUtility.loadJsonSchema(schemaBaseUri, schema);
			loadConfig.setSourceFormat("NEWLINE_DELIMITED_JSON");
		} else {
			// TODO(frew): Support for multiple schemas?
			loadSchema(schemaBaseUri, schema);
		}
		loadConfig.setSchema(schema);

		TableReference table = new TableReference();
		table.setProjectId(bigqueryProjectId);
		table.setDatasetId(bigqueryDatasetId);
		table.setTableId(bigqueryTableId);
		loadConfig.setDestinationTable(table);
		// experimental...
		loadConfig.setMaxBadRecords(maxErrors);

		config.setLoad(loadConfig);
		job.setConfiguration(config);
		if (null != jobId && !jobId.isEmpty()) {
			job.setId(jobId); // by Spletart Just in case, must be [a-zA-Z][\w]{0,1023}. Set the Id to disallow duplicates...http://stackoverflow.com/questions/11071916/bigquery-double-imports
		}

		logger.info("BQ job config: " + loadConfig);

		try {
			Insert insert = bigquery.jobs().insert(bigqueryProjectId, job);

			// TODO(frew): Not sure this is necessary, but monkey-see'ing the example code
			insert.setProjectId(bigqueryProjectId);
			JobReference ref = insert.execute().getJobReference();

			resp.getWriter().println("Successfully started job " + ref);
			logger.info("Import to BQ job started: " + ref.getJobId());

			String shouldDelete = req.getParameter(AnalysisConstants.DELETE_FROM_CLOUD_STORAGE_PARAM);
			if (AnalysisUtility.areParametersValid(shouldDelete)) {
				Queue taskQueue = QueueFactory.getQueue(queueName);
				taskQueue.add(
						Builder.withUrl(
								AnalysisUtility.getRequestBaseName(req) + "/deleteCompletedCloudStorageFilesTask")
								.method(Method.GET)
								.param(AnalysisConstants.BIGQUERY_JOB_ID_PARAM, ref.getJobId())
								.param(AnalysisConstants.QUEUE_NAME_PARAM, queueName)
								.param(AnalysisConstants.BIGQUERY_PROJECT_ID_PARAM, bigqueryProjectId));
			}
		} catch (Exception e) {
			// by sasa: catch any exception to prevent duplicates (ugly but...) - better gaps than dups!!!
			// There were at least 2 cases when exception restarted the task but import was already completed on BQ causing dups!
			// Another mystery: why GAE task is restarted despite maxretrycount is set to 0???
			logger.warning("Import job error: " + e.getMessage());
		}

	}

	private void loadSchema(String fileUri, TableSchema schema) throws IOException {
		// TODO(frew): Move to AnalysisUtility
		String schemaFileUri = fileUri + ".schema";
		String schemaFileName = "/gs/" + schemaFileUri.substring(schemaFileUri.indexOf("//") + 2);

		String schemaLine = AnalysisUtility.loadSchemaStr(schemaFileName);

		String[] schemaFieldStrs = schemaLine.split(",");
		List<TableFieldSchema> schemaFields = new ArrayList<TableFieldSchema>(schemaFieldStrs.length);
		for (String schemaFieldStr : schemaFieldStrs) {
			TableFieldSchema field = new TableFieldSchema();
			String[] schemaFieldStrParts = schemaFieldStr.split(":");
			field.setName(schemaFieldStrParts[0]);
			field.setType(schemaFieldStrParts[1]);
			field.setMode("NULLABLE");
			schemaFields.add(field);
		}

		schema.setFields(schemaFields);
	}
}
