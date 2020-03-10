
<p align="center">
  <img width="460" height="100" src="logo.jpg">
</p>

# Cog

Cog is a system to execute Datalog queries in Apache Flink.


### Prerequisites

* Java 8 (or later)
* Flink APIs

### Example

The following is an example program that shows how Datalog queries can be written in order to be executed on Apache Flink:

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

Tested on a cluster of 9 machines (8 Flink TaskManagers and 1 Flink JobManager). 
The following table shows test results: 

<TODO: insert table>

## Deployment

It can be deployed on a (standalone or YARN) cluster or on a single machine. 

## Built With

* [Maven](https://maven.apache.org/) - Dependency Management


## License

This project is licensed under Apache 2.0.