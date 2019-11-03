package org.apache.flink.datalog.parser.graph;

public abstract class Vertex {
	private String label;

	public Vertex(String label) {
		this.label = label;
	}

	public abstract PredicateData getTopPredicateData();

	@Override
	public String toString() {
		return "Vertex{" +
			"label='" + label + '\'' +
			'}';
	}
}
