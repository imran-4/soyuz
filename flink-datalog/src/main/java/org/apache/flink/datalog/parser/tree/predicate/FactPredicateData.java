package org.apache.flink.datalog.parser.tree.predicate;

import java.util.List;

public class FactPredicateData extends SimplePredicateData {

	public FactPredicateData(String predicateName, List<TermData<?>> predicateParameters) {
		super(predicateName, predicateParameters, true);
	}




}
