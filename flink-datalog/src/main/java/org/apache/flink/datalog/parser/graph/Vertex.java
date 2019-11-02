package org.apache.flink.datalog.parser.graph;

import org.jgrapht.graph.DefaultEdge;

public class Vertex {
	private String label;
	private RuleData ruleData;

	public Vertex(String label, RuleData ruleData) {
		this.label = label;
		this.ruleData = ruleData;
	}

	public String getLabel() {
		return label;
	}

	public RuleData getRuleData() {
		return ruleData;
	}

	@Override
	public String toString() {
		return "Vertex{" +
			"label='" + label + '\'' +
			", ruleData=" + ruleData +
			'}';
	}
}
