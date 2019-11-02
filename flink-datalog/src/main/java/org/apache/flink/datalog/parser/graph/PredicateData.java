package org.apache.flink.datalog.parser.graph;

import java.util.List;

public class PredicateData{
	private String predicateName;
	private List<String> predicateParameters;

	public String getPredicateName() {
		return predicateName;
	}

	public List<String> getPredicateParameters() {
		return predicateParameters;
	}

	public PredicateData(String predicateName, List<String> predicateParameters) {
		this.predicateName = predicateName;
		this.predicateParameters = predicateParameters;
	}

	@Override
	public String toString() {
		return "PredicateData{" +
			"predicateName='" + predicateName + '\'' +
			", predicateParameters=" + predicateParameters +
			'}';
	}
}
