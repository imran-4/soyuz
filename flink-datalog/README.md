# Datalog Programs Execution in Apache Flink



### Example
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env);
DataSource<Tuple2<String, String>> dataSet = env.fromElements(new Tuple2<String, String>("a", "b"),new Tuple2<String, String>("b", "c"),new Tuple2<String, String>("c", "c"),new Tuple2<String, String>("c", "d"));
datalogEnv.registerDataSet("graph", dataSet, "v1, v2");
String inputProgram = 
        "tc(X,Y) :- graph(X, Y).\n" +
		"tc(X,Y) :- tc(X,Z),graph(Z,Y).";
String query = "tc(X,Y)?";
Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
try {
	datalogEnv.toDataSet(queryResult, dataSet.getType()).collect();
} catch (Exception e) {
	e.printStackTrace();
}
```
