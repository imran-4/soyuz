package org.apache.flink.datalog.examples.aggregates;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.datalog.BatchDatalogEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.IntValue;

public class PYMK {
	public static void main(String[] args) throws Exception {
		String testFilePath = null;

		if (args.length > 0) {
			testFilePath = args[0].trim();
		} else
			throw new Exception("Please provide input dataset. ");

		String inputProgram = "uarc(X, Y) :- arc(X, Y).\n"
			+ "uarc(Y, X) :- arc(X, Y).\n"
			+ "cnt(Y, Z, count<X>) :- uarc(X, Y), uarc(X, Z), Y!= Z, ~uarc(Y, Z).\n"
			+ "pymk(X, W9, topk<10, Z>) :- cnt(X, 15, Z), pages(X, W2,W3, W9).";
		String query = "pymk(X,Y,Z)";
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings
			.newInstance()
			.useDatalogPlanner()
			.inBatchMode()
			.build();
		BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
		DataSet<Tuple2<IntValue, IntValue>> dataSet = env
			.readCsvFile(testFilePath)
			.fieldDelimiter(",")
			.types(IntValue.class, IntValue.class);

		datalogEnv.registerDataSet("graph", dataSet, "v1,v2");
		Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
		DataSet<Tuple2<IntValue, IntValue>> resultDS = datalogEnv.toDataSet(
			queryResult,
			dataSet.getType());
		resultDS.writeAsCsv(testFilePath + "_output");
		System.out.println(resultDS.count());
	}
}
