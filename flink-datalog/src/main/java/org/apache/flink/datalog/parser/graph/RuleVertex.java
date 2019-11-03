package org.apache.flink.datalog.parser.graph;

class RuleVertex extends Vertex {
	private RuleData ruleData;

	RuleVertex(String label, RuleData ruleData) {
		super(label);
		this.ruleData = ruleData;
	}

	public RuleData getRuleData() {
		return ruleData;
	}

	@Override
	public PredicateData getTopPredicateData() {
		return this.ruleData.getHeadPredicate();
	}

	@Override
	public String toString() {
		return "RuleVertex{" +
			"ruleData=" + ruleData +
			'}';
	}
}
