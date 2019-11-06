package org.apache.flink.datalog.parser.tree.predicate;


import java.util.List;

public class PrimitivePredicateData extends PredicateData {
	private TermData leftTerm;
	private String operator;
	private TermData rightTerm;

	public PrimitivePredicateData(TermData leftTerm, String operator, TermData rightTerm) {
		this.leftTerm = leftTerm;
		this.operator = operator;
		this.rightTerm = rightTerm;
	}

	public TermData getLeftTerm() {
		return leftTerm;
	}

	public String getOperator() {
		return operator;
	}

	public TermData getRightTerm() {
		return rightTerm;
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
			"leftTerm=" + leftTerm +
			", operator='" + operator + '\'' +
			", rightTerm=" + rightTerm +
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
