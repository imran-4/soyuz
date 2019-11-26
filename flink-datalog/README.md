# Datalog Programs Execution in Apache Flink

TODO

## Getting Started
TODO

### Prerequisites

TODO

```
TODO
```


### Example
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env);
DataSource<Tuple2<String, String>> dataSet = ... // create a dataset
datalogEnv.registerDataSet("<name>", dataSet, "<fields (comma separated )>");
String inputProgram = 
        "tc(X,Y) :- graph(X, Y).\n" +
		"tc(X,Y) :- tc(X,Z),graph(Z,Y).";   //transitive closure example
String query = "tc(X,Y)?";
Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
try {
	datalogEnv.toDataSet(queryResult, dataSet.getType()).collect();
} catch (Exception e) {
	e.printStackTrace();
}
```

### Experiments

TODO

## Deployment

TODO

## Built With

* [Maven](https://maven.apache.org/) - Dependency Management

## Contributing

TODO

## Authors

TODO

## License

TODO
