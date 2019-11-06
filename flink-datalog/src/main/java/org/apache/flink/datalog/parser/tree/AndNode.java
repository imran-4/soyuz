package org.apache.flink.datalog.parser.tree;

import org.apache.flink.datalog.parser.tree.predicate.PredicateData;

import java.util.ArrayList;
import java.util.List;

public class AndNode extends Node {
	private PredicateData predicateData;
	private List<OrNode> children = new ArrayList<>();
	private boolean isRecursive = false;

	public AndNode(PredicateData predicateData) {
		this.predicateData = predicateData;
	}

	public AndNode(PredicateData predicateData, boolean isRecursive) {
		this.predicateData = predicateData;
		this.isRecursive = isRecursive;
	}

	public void setRecursive(boolean isRecursive) {
		this.isRecursive = isRecursive;
	}

	public boolean isRecursive() {
		return this.isRecursive;
	}

	@Override
	public List<OrNode> getChildren() {
		return children;
	}

	void setChildren(List<OrNode> children) {
		this.children = children;
	}

	@Override
	public PredicateData getPredicateData() {
		return predicateData;
	}

	@Override
	public int getChildrenCount() {
		return this.children.size();
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
			", isRecursive=" + isRecursive +
			'}';
	}

	@Override
	public <T> T accept(TreeVisitor<? extends T> visitor) {
		if (visitor instanceof AndOrTreeVisitor) return ((AndOrTreeVisitor<? extends T>) visitor).visitAndNode(this);
		else return visitor.visitChildren(this);
	}
}
