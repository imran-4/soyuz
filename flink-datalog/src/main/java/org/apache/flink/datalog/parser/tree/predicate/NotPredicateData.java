package org.apache.flink.datalog.parser.tree.predicate;

import java.util.List;

public class NotPredicateData extends SimplePredicateData{

	public NotPredicateData(String predicateName, List<TermData<?>> predicateParameters) {
		super(predicateName, predicateParameters, false);
	}
}
