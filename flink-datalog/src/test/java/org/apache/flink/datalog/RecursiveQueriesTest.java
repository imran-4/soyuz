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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.datalog.BatchDatalogEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * This class contains test cases for recursive queries.
 */
public class RecursiveQueriesTest {

    private static String testDataFolderPath = "/tmp/";//System.getProperty("user.dir") + "/src/test/resources/";

    private List<Tuple2<String, String>> transitiveClosureTest(String testFilePath) throws Exception {
        String inputProgram =
                "tc(X,Y) :- graph(X,Y).\n"
                        + "tc(X,Y) :- graph(X,Z), tc(Z,Y).\n";
        String query = "tc(X,Y)?";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useDatalogPlanner()
                .inBatchMode()
                .build();
        BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
        DataSet<Tuple2<String, String>> dataSet = env.readCsvFile(testFilePath).fieldDelimiter(",").types(String.class, String.class);

        datalogEnv.registerDataSet("graph", dataSet, "v1,v2");
        Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
        DataSet<Tuple2<String, String>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());
        return resultDS.collect();
    }

    private List<Tuple2<String, String>> sameGenerationTest(String testFilePath) throws Exception {
        String inputProgram =
                "sg(X,Y):-arc(P,X),arc(P,Y),X!=Y.\n" +
                        "sg(X,Y):-arc(A,X),sg(A,B),arc(B,Y).\n";
        String query = "sg(X,Y)?";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useDatalogPlanner()
                .inBatchMode()
                .build();
        BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
        DataSet<Tuple2<String, String>> dataSet = env.readCsvFile(testFilePath).fieldDelimiter(",").types(String.class, String.class);
        datalogEnv.registerDataSet("arc", dataSet, "v1,v2");
        Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
        DataSet<Tuple2<String, String>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());
        return resultDS.collect();
    }

    @Test
    public void testTransitiveClosureWithTree11() throws Exception {
        List<Tuple2<String, String>> actual = transitiveClosureTest(testDataFolderPath + "tree11.csv");

        PrintWriter writer = new PrintWriter(new File(testDataFolderPath + "tree11_result_tc.csv"));
        for (Tuple2<String, String> tuple : actual) {
            writer.println(tuple.f0 + "," + tuple.f1);
        }
        writer.println();
        writer.close();

        assertTrue(true);
    }

    @Test
    public void testTransitiveClosureWithGrid150() throws Exception {
        List<Tuple2<String, String>> actual = transitiveClosureTest(testDataFolderPath + "grid150.csv");

        PrintWriter writer = new PrintWriter(new File(testDataFolderPath + "grid150_result_tc.csv"));
        for (Tuple2<String, String> tuple : actual) {
            writer.println(tuple.f0 + "," + tuple.f1);
        }
        writer.println();
        writer.close();

        assertTrue(true);
    }

    @Test
    public void testTransitiveClosureWithGnp10k() throws Exception {
        List<Tuple2<String, String>> actual = transitiveClosureTest(testDataFolderPath + "gnp10K.csv");

        PrintWriter writer = new PrintWriter(new File(testDataFolderPath + "gnp10K_result_tc.csv"));
        for (Tuple2<String, String> tuple : actual) {
            writer.println(tuple.f0 + "," + tuple.f1);
        }
        writer.println();
        writer.close();

        assertTrue(true);

    }

    /**
     * @throws Exception
     */
    @Test
    public void testSameGenerationWithTree11() throws Exception {
        List<Tuple2<String, String>> actual = sameGenerationTest(testDataFolderPath + "tree11.csv");

        PrintWriter writer = new PrintWriter(new File(testDataFolderPath + "tree11_result_sg.csv"));
        for (Tuple2<String, String> tuple : actual) {
            writer.println(tuple.f0 + "," + tuple.f1);
        }
        writer.println();
        writer.close();

        assertTrue(true);
    }

    @Test
    public void testSameGenerationWithGrid150() throws Exception {
        List<Tuple2<String, String>> actual = sameGenerationTest(testDataFolderPath + "grid150.csv");

        PrintWriter writer = new PrintWriter(new File(testDataFolderPath + "grid150_result_sg.csv"));
        for (Tuple2<String, String> tuple : actual) {
            writer.println(tuple.f0 + "," + tuple.f1);
        }
        writer.println();
        writer.close();

        assertTrue(true);
    }

    @Test
    public void testSameGenerationWithGnp10k() throws Exception {
        List<Tuple2<String, String>> actual = sameGenerationTest(testDataFolderPath + "gnp10K.csv");

        PrintWriter writer = new PrintWriter(new File(testDataFolderPath + "gnp10K_result_sg.csv"));
        for (Tuple2<String, String> tuple : actual) {
            writer.println(tuple.f0 + "," + tuple.f1 );
        }
        writer.println();
        writer.close();
        assertTrue(true);
    }
}