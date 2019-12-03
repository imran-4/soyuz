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

package org.apache.flink;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.datalog.BatchDatalogEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class RecursiveQueriesTest {
	private static BatchDatalogEnvironment datalogEnv;
	private static DataSource<Tuple2<String, String>> dataSet;

	/**
	 *
	 */
	@BeforeClass
	public static void initEnvs() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings
			.newInstance()
			.useDatalogPlanner()
			.inBatchMode()
			.build();
		datalogEnv = BatchDatalogEnvironment.create(env, settings);
		dataSet = env.fromElements(
			new Tuple2<>("a", "b"),
			new Tuple2<>("b", "c"),
			new Tuple2<>("c", "c"),
			new Tuple2<>("c", "d")); //may be we need different datasets for each test...
		datalogEnv.registerDataSet("graph", dataSet, "v1,v2");
	}

	/**
	 *
	 * @throws Exception
	 */
	@Test
	public void transitiveClosureTest() throws Exception {
		String inputProgram = "tc(X,Y) :- graph(X,Y).\n"
			+ "tc(X,Y) :- graph(X,Z), tc(Z,Y).\n";
		String query = "tc(X,Y)?";
		Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
		DataSet<Tuple2<String, String>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());
		List<Tuple2<String, String>> transitiveClosureActual = resultDS.collect();
		List<Tuple2<String, String>> transitiveClosureExpected = List
			.of(new Tuple2<>("", ""),
				new Tuple2<>("", ""),
				new Tuple2<>("", ""),
				new Tuple2<>("", ""),
				new Tuple2<>("", ""));
		assertEquals(transitiveClosureActual, transitiveClosureExpected);
	}

	/**
	 *
	 * @throws Exception
	 */
	@Test
	public void sameGenerationTest() throws Exception {
		String inputProgram = "sg(X,Y):-graph(P,X),graph(P,Y),X!=Y.\n" +
			"sg(X,Y):-graph(A,X),sg(A,B),graph(X,Y).\n";
		String query = "sg(X,Y)?";
		Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
		DataSet<Tuple2<String, String>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());
		List<Tuple2<String, String>> transitiveClosureActual = resultDS.collect();
		List<Tuple2<String, String>> transitiveClosureExpected = List
			.of(new Tuple2<>("", ""),
				new Tuple2<>("", ""),
				new Tuple2<>("", ""),
				new Tuple2<>("", ""),
				new Tuple2<>("", ""));
		assertEquals(transitiveClosureActual, transitiveClosureExpected);
	}

	/**
	 *
	 * @throws Exception
	 */
	@Test
	public void peopleYouMayKnowTest() throws Exception {
		String inputProgram = ""; //todo
		String query = "";
		Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
		DataSet<Tuple2<String, String>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());
		List<Tuple2<String, String>> transitiveClosureActual = resultDS.collect();
		List<Tuple2<String, String>> transitiveClosureExpected = List
			.of(new Tuple2<>("", ""),
				new Tuple2<>("", ""),
				new Tuple2<>("", ""),
				new Tuple2<>("", ""),
				new Tuple2<>("", ""));
		assertEquals(transitiveClosureActual, transitiveClosureExpected);
	}

	/**
	 *
	 * @throws Exception
	 */
	@Test
	public void connectedComponentsTest() throws Exception {
		String inputProgram = ""; //todo
		String query = "";
		Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
		DataSet<Tuple2<String, String>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());
		List<Tuple2<String, String>> transitiveClosureActual = resultDS.collect();
		List<Tuple2<String, String>> transitiveClosureExpected = List
			.of(new Tuple2<>("", ""),
				new Tuple2<>("", ""),
				new Tuple2<>("", ""),
				new Tuple2<>("", ""),
				new Tuple2<>("", ""));
		assertEquals(transitiveClosureActual, transitiveClosureExpected);
	}
}
