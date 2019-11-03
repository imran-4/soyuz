package org.apache.flink.datalog.parser.graph;

class QueryVertex extends Vertex {
	private PredicateData queryPreidcateData;

	public QueryVertex(String label, PredicateData queryPreidcateData) {
		super(label);
		this.queryPreidcateData = queryPreidcateData;
	}

	@Override
	public PredicateData getTopPredicateData() {
		return this.queryPreidcateData;
	}

	@Override
	public String toString() {
		return "QueryVertex{" +
			"queryPreidcateData=" + queryPreidcateData +
			'}';
	}
}
