package org.apache.flink.datalog.parser.tree;

import org.apache.flink.datalog.parser.tree.predicate.PredicateData;

import java.util.ArrayList;
import java.util.List;

public class OrNode extends Node {
	private PredicateData predicateData;
	private List<? extends AndNode> children = new ArrayList<>();

	public OrNode(PredicateData predicateData) {
		this.predicateData = predicateData;
	}

	public List<? extends AndNode> getChildren() {
		return children;
	}

	public void setChildren(List<? extends AndNode> children) {
		this.children = children;
	}

	public PredicateData getPredicateData() {
		return predicateData;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		OrNode orNode = (OrNode) o;
		return predicateData.equals(orNode.predicateData);
	}

	@Override
	public String toString() {
		return "OrNode{" +
			"predicateData=" + predicateData +
			", children=" + children +
			'}';
	}
}
