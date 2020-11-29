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

package org.apache.flink.datalog.examples.streaming;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.datalog.BatchDatalogEnvironment;
import org.apache.flink.datalog.streaming.StreamingDatalogEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.IntValue;

public class TransitiveClosure {
    public static void main(String[] args) throws Exception {
        String testFilePath = null;

        if (args.length > 0) {
            testFilePath = args[0].trim();
        } else
            throw new Exception("Please provide input dataset. ");
        String inputProgram =
                          "tc(X,Y) :- graph(X,Y).\n"
                        + "tc(X,Y) :- tc(X,Z),graph(Z,Y).\n";
        String query = "tc(X,Y)?";

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useDatalogPlanner()
                .inStreamingMode()
                .build();
		StreamingDatalogEnvironment datalogEnv = StreamingDatalogEnvironment.create(env, settings);
        DataStream<Tuple2<String, String>> dataSet = env.readTextFile("")
			.map(x -> new Tuple2<>(x.split(",")[0],x.split(",")[1]));
			//env.readCsvFile(testFilePath).fieldDelimiter(",").types(IntValue.class, IntValue.class);

        datalogEnv.createTemporaryView("graph", dataSet, "v1,v2");
        Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
        DataStream<Tuple2<Boolean, Tuple2<String, String>>> resultDS = datalogEnv.toRetractStream(queryResult, dataSet.getType());
        resultDS.writeAsCsv(testFilePath+"_output");
        System.out.println(resultDS.addSink(new SinkFunction<Tuple2<Boolean, Tuple2<String, String>>>() {
			@Override
			public void invoke(
				Tuple2<Boolean, Tuple2<String, String>> value,
				Context context) throws Exception {

			}
		}));
    }
}
