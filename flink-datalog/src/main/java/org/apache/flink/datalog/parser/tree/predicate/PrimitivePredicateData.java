package org.apache.flink.datalog.parser.tree.predicate;


import java.util.List;

public class PrimitivePredicateData extends PredicateData {
	private String expressions; //for now use String

	public TermData getLeftTerm() {
		return leftTerm;
	}

	public String getOperator() {
		return operator;
	}

	public TermData getRightTerm() {
		return rightTerm;
	}

	private TermData leftTerm;
	private String operator;
	private TermData rightTerm;

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

	public enum Operators {
		EQUALS,
		NOT_EQUALS,
		LESS_THAN,
		GREATER_THAN,
		//...
	}
}
