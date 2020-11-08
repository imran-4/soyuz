package org.apache.flink.datalog.examples.aggregates;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.datalog.BatchDatalogEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;

public class TriangleCount {
	public static void main(String[] args) throws Exception {
		String testFilePath = null;

		if (args.length > 0) {
			testFilePath = args[0].trim();
		} else
			throw new Exception("Please provide input dataset. ");
		String inputProgram = "triangles(X, Y, Z) :- arc(X, Y), X < Y, arc(Y, Z), Y < Z, arc(Z, X).\n"
			+ "count_triangles(count<_>) :- triangles(X, Y, Z).\n";

		String query = "count_triangles(X)?";

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings
			.newInstance()
			.useDatalogPlanner()
			.inBatchMode()
			.build();
		BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
		DataSet<Tuple2<Integer, Integer>> dataSet = env.readCsvFile(testFilePath).fieldDelimiter(",").types(Integer.class, Integer.class);

		datalogEnv.registerDataSet("arc", dataSet, "v1,v2");
		Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
		DataSet<Tuple2<Integer, Integer>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());

//        resultDS.writeAsCsv(testFilePath+"_output");
		System.out.println(resultDS.count());
	}
}
