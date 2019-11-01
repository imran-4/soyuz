package org.apache.flink.datalog.parser.tree.predicate;


import java.util.List;

public class PrimitivePredicateData extends PredicateData {
	private String expressions; //for now use String

	public PrimitivePredicateData(String expressions) {
		this.expressions = expressions;
	}

	public String getExpressions() {
		return expressions;
	}

	@Override
	public String getPredicateName() {
		return null;
	}

	@Override
	public List<TermData> getPredicateParameters() {
		return null;
	}

	@Override
	public String toString() {
		return "PrimitivePredicateData{" +
			"expressions='" + expressions + '\'' +
			'}';
	}
}
