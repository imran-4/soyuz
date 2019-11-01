package org.apache.flink.datalog.parser.tree.predicate;

import java.util.List;

public class QueryPredicateData extends SimplePredicateData {
	public QueryPredicateData(String predicateName, List<TermData> predicateParameters) {
		super(predicateName, predicateParameters);
	}

	@Override
	public String toString() {
		return "QueryPredicateData{" + super.toString() + "}";
	}
}
