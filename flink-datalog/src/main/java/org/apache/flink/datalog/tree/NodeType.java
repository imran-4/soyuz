package org.apache.flink.datalog.tree;

public enum NodeType {
	LITERAL,
	VARIABLE,
	FACT,
	RULE,
	SCHEMA,
	QUERY,
	PREDICATE,
	TERM
	//etc..
}
