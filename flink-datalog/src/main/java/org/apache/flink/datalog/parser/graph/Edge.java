package org.apache.flink.datalog.parser.graph;

import org.jgrapht.graph.DefaultEdge;

public class Edge extends DefaultEdge {
	private String label;

	Edge(String label) {
		this.label = label;
	}

	public String getLabel() {
		return label;
	}

	@Override
	public String toString() {
		return "Edge{" +
			"label='" + label + '\'' +
			'}';
	}
}
