package org.apache.flink.datalog.examples;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.datalog.BatchDatalogEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.IntValue;

public class WhoAttendParty {
	public static void main(String[] args) throws Exception {
		String testFilePath = null;

		if (args.length > 0) {
			testFilePath = args[0].trim();
		} else
			throw new Exception("Please provide input dataset. ");

		String inputProgram = "cntComing(Y, mcount<X>) :- attend(X), friend(Y, X).\n"
			+ "attend(X) :- organizer(X).\n"
			+ "attend(X) :- cntComing(X, N), N >= 3.\n";
		String query = "attend(X)?";
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
