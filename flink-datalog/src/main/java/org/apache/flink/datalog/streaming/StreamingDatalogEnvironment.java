/*
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.flink.datalog.streaming;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.datalog.DatalogEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.api.scala.BatchTableEnvironment;

/**
 *
 */
public interface StreamingDatalogEnvironment extends DatalogEnvironment, StreamTableEnvironment {
	ExecutionEnvironment ENV = null;

	static StreamingDatalogEnvironment create(
		ExecutionEnvironment executionEnvironment,
		EnvironmentSettings environmentSettings) {
		return create(executionEnvironment, environmentSettings, new TableConfig());
	}

	static StreamingDatalogEnvironment create(ExecutionEnvironment executionEnvironment) {
		return create(executionEnvironment, EnvironmentSettings.newInstance().build());
	}

	static StreamingDatalogEnvironment create(
		ExecutionEnvironment executionEnvironment,
		EnvironmentSettings environmentSettings,
		TableConfig tableConfig) {
		return null;
	}

	Table datalogQuery(String inputProgram, String query);
}
