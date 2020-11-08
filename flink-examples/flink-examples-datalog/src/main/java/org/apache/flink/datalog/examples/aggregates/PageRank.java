package org.apache.flink.datalog.examples.aggregates;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.datalog.BatchDatalogEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.IntValue;

public class PageRank {
	public static void main(String[] args) throws Exception {
		String testFilePath = null;

		if (args.length > 0) {
			testFilePath = args[0].trim();
		} else
			throw new Exception("Please provide input dataset. ");
		String inputProgram = "Outdegree(X,count<Y>) :- Edge(X,Y).\n"+
			"PageRank(X,0,1).\n"+
			"PageRank(X,I+ 1,sum<P/D>) :- Edge(Y,X),PageRank(Y,I,P),Outdegree(Y,D),I<20.\n";

		String query = "PageRank(X,Y,Z)?";

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings
			.newInstance()
			.useDatalogPlanner()
			.inBatchMode()
			.build();
		BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
		DataSet<Tuple2<Integer, Integer>> dataSet = env.readCsvFile(testFilePath).fieldDelimiter(",").types(Integer.class, Integer.class);

		datalogEnv.registerDataSet("Edge", dataSet, "v1,v2");
		Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
		DataSet<Tuple2<Integer, Integer>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());

//        resultDS.writeAsCsv(testFilePath+"_output");
		System.out.println(resultDS.count());
	}
}
