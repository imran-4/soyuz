package org.apache.flink.datalog.parser.graph;

import java.util.List;

public class PredicateData {
	private String predicateName;
	private List<TermData> predicateParameters;

	PredicateData(String predicateName, List<TermData> predicateParameters) {
		this.predicateName = predicateName;
		this.predicateParameters = predicateParameters;
	}

	public String getPredicateName() {
		return predicateName;
	}

	public List<TermData> getPredicateParameters() {
		return predicateParameters;
	}

	@Override
	public String toString() {
		return "PredicateData{" +
			"predicateName='" + predicateName + '\'' +
			", predicateParameters=" + predicateParameters +
			'}';
	}
}
