package org.apache.flink.datalog.parser.tree;

import org.apache.flink.datalog.parser.tree.predicate.PredicateData;

import java.util.ArrayList;
import java.util.List;

public class AndNode extends Node{
	private PredicateData predicateData;
	private List<OrNode> children = new ArrayList<>();

	public AndNode(PredicateData predicateData) {
		this.predicateData = predicateData;
	}

	public List<OrNode> getChildren() {
		return children;
	}

	public void setChildren(List<OrNode> children) {
		this.children = children;
	}

	public PredicateData getPredicateData() {
		return predicateData;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		AndNode andNode = (AndNode) o;
		return predicateData.equals(andNode.predicateData);
	}

	@Override
	public String toString() {
		return "AndNode{" +
			"predicateData=" + predicateData +
			", children=" + children +
			'}';
	}
}
