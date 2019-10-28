package org.apache.flink.datalog.parser.tree.graph;

import org.jgrapht.graph.DefaultEdge;

public class Edge extends DefaultEdge {
	private String label;

	public Edge(String label) {
		this.label = label;
	}

	@Override
	public String toString() {
		return "Edge{" +
			"label='" + label + '\'' +
			'}';
	}

	public String getLabel() {
		return label;
	}
}
