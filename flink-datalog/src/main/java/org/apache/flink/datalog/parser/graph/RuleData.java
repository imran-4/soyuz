package org.apache.flink.datalog.parser.graph;

import java.util.List;

public class RuleData {
	private int ruleId;
	private PredicateData headPredicate;
	private List<PredicateData> bodyPredicate;

	RuleData(int ruleId, PredicateData headPredicate, List<PredicateData> bodyPredicate) {
		this.ruleId = ruleId;
		this.headPredicate = headPredicate;
		this.bodyPredicate = bodyPredicate;
	}

	public int getRuleId() {
		return ruleId;
	}

	public PredicateData getHeadPredicate() {
		return headPredicate;
	}

	public List<PredicateData> getBodyPredicate() {
		return bodyPredicate;
	}

	@Override
	public String toString() {
		return "RuleData{" +
			"ruleId='" + ruleId + '\'' +
			", headPredicate=" + headPredicate +
			", bodyPredicate=" + bodyPredicate +
			'}';
	}
}
