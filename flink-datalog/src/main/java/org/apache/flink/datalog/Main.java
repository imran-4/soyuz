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

package org.apache.flink.datalog;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;

import java.util.List;

//this file is only for testing... it has to be removed later.
public class Main {
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useDatalogPlanner().inBatchMode().withBuiltInCatalogName("my_catalog").withBuiltInDatabaseName("my_database").build();
		BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
		DataSource<Tuple2<String, String>> dataSet = env.fromElements(
			new Tuple2<>("a", "b"),
			new Tuple2<>("b", "c"),
			new Tuple2<>("c", "c"),
			new Tuple2<>("c", "d")); //will also support loading data using facts. e.g., fact(a,b). fact(b,c).
		datalogEnv.registerDataSet("graph", dataSet, "v1,v2"); //register EDB. EDBs are registered explicitly.
		DataSource<Tuple2<String, String>> dataSet1 = env.fromElements(
			new Tuple2<>("b", "1"),
			new Tuple2<>("c", "1"),
			new Tuple2<>("c", "1"),
			new Tuple2<>("d", "1")); //just to test the query logical plan...will remove it later
		datalogEnv.registerDataSet("tc", dataSet1, "v2,v3");

		String inputProgram =
			"tc(X,Y) :- graph(X,Y).\n" +
				"tc(X,Y) :- tc(X,Y).";
		String query = "tc(X,Y)?";

		String inputProgram2 = "sg(X,Y):-arc(P,X),arc(P,Y),X!=Y.\n" +
			"arc1(X,Y):- arc(Z,Y).\n" +
			"arc1(X,Y):- arc1(X,Z), arc(Z,Y).\n" +
			"sg(X,Y):-arc(A,X),sg(A,B),arc1(B,Y).\n";
		String query2 = "sg(X,Y)?";

		datalogEnv.registerDataSet("arc", dataSet1, "v1,v2");
		String inputProgram3 = "sg(X,Y):-arc(P,X),arc(P,Y),X!=Y.\n" +
			"sg(X,Y):-arc(A,X),sg(A,B),arc(X,Y).\n";
		String query3 = "sg(X,Y)?";

		String inputProgram4 =  "tc(X,Y):-arc(X,Y).\n"+ "tc(X,Y):-arc(X,Z), arc(Z,Y), 1=1.";
		String query4 = "tc(X,Y)?"; // simple "select v1,v2 from graph   " query (no recursion involved).

		Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
		List<Tuple2<String, String>> collectedData = null;
		try {
			DataSet<Tuple2<String, String>> dataSet2 = datalogEnv.toDataSet(queryResult, dataSet1.getType());
			collectedData = dataSet2.collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("-------------------");
		collectedData.forEach(System.out::println);
	}
}
