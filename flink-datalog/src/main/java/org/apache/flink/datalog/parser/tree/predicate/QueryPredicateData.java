package org.apache.flink.datalog.parser.tree.predicate;

import java.util.List;

public class QueryPredicateData extends SimplePredicateData {
	public QueryPredicateData(String predicateName, List<String> predicateParameters) {
		super(predicateName, predicateParameters);
	}

	@Override
	public String toString() {
		return "QueryPredicateData{}";
	}
}
