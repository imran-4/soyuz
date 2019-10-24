package org.apache.flink.datalog;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;

public class Main {
	//for testing only
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useDatalogPlanner().inBatchMode().withBuiltInCatalogName("my_catalog").withBuiltInDatabaseName("my_database").build();
		BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
		DataSource<Tuple2<String, String>> dataSet = env.fromElements(
			new Tuple2<>("a", "b"),
			new Tuple2<>("b", "c"),
			new Tuple2<>("c", "c"),
			new Tuple2<>("c", "d")); //will also support loading data using facts. e.g., fact(a,b). fact(b,c).
		datalogEnv.registerDataSet("graph", dataSet, "v1, v2"); //register EDB. EDBs are registered explicitly.

		DataSource<Tuple3<String, String, String>> dataSet1 = env.fromElements(
			new Tuple3<>("a", "b", "1"),
			new Tuple3<>("b", "c", "1"),
			new Tuple3<>("c", "c", "1"),
			new Tuple3<>("c", "d", "1")); //just to test the query logical plan...will remove it later
		datalogEnv.registerDataSet("abc", dataSet1, "v1, v2, v3"); // dont know whether we need to register IDBs
		String inputProgram =
			"abc(X,Y) :- graph(X, Y).\n" +
				"abc(X,Y) :- abc(X,Z),graph(Z,Y).";
//		datalogEnv.evaluateDatalogRules(inputProgram);
		Table queryResult = datalogEnv.datalogQuery("abc(a,b)?");
		try {
			datalogEnv.toDataSet(queryResult, Tuple2.class).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
