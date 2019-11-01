package org.apache.flink.datalog.parser.tree.predicate;

import java.util.List;

public abstract class PredicateData {
	private int predicateID;

	public abstract String getPredicateName();

	public abstract List<TermData> getPredicateParameters();

	@Override
	public String toString() {
		return "PredicateData{" +
			"predicateID=" + predicateID +
			'}';
	}
}
