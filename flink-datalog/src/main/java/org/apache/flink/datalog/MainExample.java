package org.apache.flink.datalog;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.IntValue;

public class MainExample {
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

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings
			.newInstance()
			.useDatalogPlanner()
			.inBatchMode()
			.build();
		BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
		DataSet<Tuple2<IntValue, IntValue>> dataSet = env.readCsvFile(testFilePath).fieldDelimiter(",").types(IntValue.class, IntValue.class);

		datalogEnv.registerDataSet("graph", dataSet, "v1,v2");
		Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
		DataSet<Tuple2<IntValue, IntValue>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());
		resultDS.writeAsCsv(testFilePath+"_output");
		System.out.println(resultDS.count());
	}
}
