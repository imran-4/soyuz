package org.apache.flink.datalog.parser.tree;

import org.apache.flink.datalog.DatalogVisitor;
import org.apache.flink.datalog.parser.tree.predicate.PredicateData;
import org.apache.flink.datalog.plan.logical.AndOrTreeVisitor;
import org.apache.flink.datalog.plan.logical.TreeVisitor;

import java.util.ArrayList;
import java.util.List;

public class OrNode extends Node {
	private PredicateData predicateData;
	private List<AndNode> children = new ArrayList<>();
	private boolean isRecursive = false;

	public void setRecursive(boolean recursive) {
		this.isRecursive = recursive;
	}

	public boolean isRecursive() {
		return isRecursive;
	}

	public OrNode(PredicateData predicateData) {
		this.predicateData = predicateData;
		this.isRecursive = false;
	}

	public OrNode(PredicateData predicateData, boolean isRecursive) {
		this.predicateData = predicateData;
		this.isRecursive = isRecursive;
	}

	@Override
	public List<AndNode> getChildren() {
		return children;
	}

	public void setChildren(List<AndNode> children) {
		this.children = children;
	}

	@Override
	public int getChildrenCount() {
		return this.children.size();
	}

	@Override
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

	@Override
	public <T> T accept(TreeVisitor<? extends T> visitor) {
		if ( visitor instanceof AndOrTreeVisitor) return ((AndOrTreeVisitor<? extends T>)visitor).visitOrNode(this);
		else return visitor.visitChildren(this);
	}
}
