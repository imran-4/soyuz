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

import org.apache.commons.collections.CollectionUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * This class contains test cases for recursive queries.
 */
@Category(RecursiveTests.class)
public class RecursiveQueriesTest {
    private static ExecutionEnvironment env;
    private static BatchDatalogEnvironment datalogEnv;
    private static DataSource<Tuple2<String, String>> dataSet;

    /**
     *
     */
    @BeforeClass
    public static void initEnvs() {
        env = ExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useDatalogPlanner()
                .inBatchMode()
                .build();
        datalogEnv = BatchDatalogEnvironment.create(env, settings);
    }

    /**
     * @throws Exception
     */
    @Test
    public void testTransitiveClosure() throws Exception {
        String inputProgram = "tc(X,Y) :- graph(X,Y).\n"
                + "tc(X,Y) :- graph(X,Z), tc(Z,Y).\n";
        String query = "tc(X,Y)?";

        dataSet = env.fromElements(
                new Tuple2<>("a", "b"),
                new Tuple2<>("b", "c"),
                new Tuple2<>("c", "c"),
                new Tuple2<>("c", "d")); //may be we need different datasets for each test...
        datalogEnv.registerDataSet("graph", dataSet, "v1,v2");
        Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
        DataSet<Tuple2<String, String>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());
        List<Tuple2<String, String>> actual = resultDS.collect();
        List<Tuple2<String, String>> expected = List
                .of(new Tuple2<>("a", "b"),
                        new Tuple2<>("a", "c"),
                        new Tuple2<>("a", "d"),
                        new Tuple2<>("b", "c"),
                        new Tuple2<>("b", "d"),
                        new Tuple2<>("c", "c"),
                        new Tuple2<>("c", "d"));
        assertTrue(CollectionUtils.isEqualCollection(actual, expected));
    }

    /**
     * @throws Exception
     */
    @Test
    public void testSameGeneration() throws Exception {
        String inputProgram = "sg(X,Y):-graph(P,X),graph(P,Y),X!=Y.\n" +
                "sg(X,Y):-graph(A,X),sg(A,B),graph(X,Y).\n";
        String query = "sg(X,Y)?";

        dataSet = env.fromElements(
                new Tuple2<>("1", "2"),
                new Tuple2<>("2", "3"),
                new Tuple2<>("1", "4"),
                new Tuple2<>("2", "5"),
                new Tuple2<>("5", "4"),
                new Tuple2<>("1", "9"),
                new Tuple2<>("2", "8"));
        datalogEnv.registerDataSet("graph", dataSet, "v1,v2");
        Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
        DataSet<Tuple2<String, String>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());
        List<Tuple2<String, String>> actual = resultDS.collect();
        List<Tuple2<String, String>> expected = List
                .of(
                        new Tuple2<>("2", "4"),
                        new Tuple2<>("2", "9"),
                        new Tuple2<>("3", "5"),
                        new Tuple2<>("3", "8"),
                        new Tuple2<>("4", "2"),
                        new Tuple2<>("4", "9"),
                        new Tuple2<>("5", "3"),
                        new Tuple2<>("5", "8"),
                        new Tuple2<>("8", "3"),
                        new Tuple2<>("8", "5"),
                        new Tuple2<>("9", "2"),
                        new Tuple2<>("9", "4")
                );
        assertTrue(CollectionUtils.isEqualCollection(actual, expected));
    }

    /**
     * @throws Exception
     */
    @Test
    public void testPeopleYouMayKnow() throws Exception {
        String inputProgram = ""; //todo
        String query = "";

        dataSet = env.fromElements(
                new Tuple2<>("1", "2"),
                new Tuple2<>("2", "3"),
                new Tuple2<>("1", "4"),
                new Tuple2<>("2", "5"),
                new Tuple2<>("5", "4"),
                new Tuple2<>("1", "9"),
                new Tuple2<>("2", "8"));
        datalogEnv.registerDataSet("graph", dataSet, "v1,v2");
        Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
        DataSet<Tuple2<String, String>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());
        List<Tuple2<String, String>> actual = resultDS.collect();
        List<Tuple2<String, String>> expected = List
                .of(
                        new Tuple2<>("", ""),
                        new Tuple2<>("", ""),
                        new Tuple2<>("", ""),
                        new Tuple2<>("", ""),
                        new Tuple2<>("", ""));
        assertTrue(CollectionUtils.isEqualCollection(actual, expected));
    }

    /**
     * @throws Exception
     */
    @Test
    public void testConnectedComponents() throws Exception {
        String inputProgram = ""; //todo
        String query = "";

        dataSet = env.fromElements(
                new Tuple2<>("1", "2"),
                new Tuple2<>("2", "3"),
                new Tuple2<>("1", "4"),
                new Tuple2<>("2", "5"),
                new Tuple2<>("5", "4"),
                new Tuple2<>("1", "9"),
                new Tuple2<>("2", "8"));
        datalogEnv.registerDataSet("graph", dataSet, "v1,v2");
        Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
        DataSet<Tuple2<String, String>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());
        List<Tuple2<String, String>> actual = resultDS.collect();
        List<Tuple2<String, String>> expected = List
                .of(
                        new Tuple2<>("", ""),
                        new Tuple2<>("", ""),
                        new Tuple2<>("", ""),
                        new Tuple2<>("", ""),
                        new Tuple2<>("", ""));
        assertTrue(CollectionUtils.isEqualCollection(actual, expected));
    }
}
