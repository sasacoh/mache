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

package com.streak.logging.analysis.example;

import com.google.appengine.api.log.RequestLogs;
import com.streak.logging.analysis.BigqueryFieldExporter;

public class InstanceFieldExporter implements BigqueryFieldExporter {
	private String instanceKey;
	
	@Override
	public int processLog(RequestLogs log) {
		instanceKey = log.getInstanceKey();
		return 1;
	}

	@Override
	public Object getField(String name) {
		return instanceKey;
	}

	@Override
	public int getFieldCount() {
		return 1;
	}

	@Override
	public String getFieldName(int i) {
		return "instanceKey";
	}

	@Override
	public String getFieldType(int i) {
		return "string";
	}

	@Override
	public Object getField(String name, int row) {
		return getField(name);
	}
}
