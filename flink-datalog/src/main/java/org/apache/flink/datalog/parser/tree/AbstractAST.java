package org.apache.flink.datalog.parser.tree;

import java.util.ArrayList;
import java.util.List;

abstract class AbstractAST implements AST {
	private List<AST> children = new ArrayList<>();

	@Override
	public void addChild(AST node) {
		if (node == null) return;
		children.add(node);
	}

	@Override
	public List<AST> getChildren() {
		return this.children;
	}

	@Override
	public boolean equals(AST t) {
		if (t == null)
			return false;
		if (t.getText() != null && this.getText() !=null) {
			return t.getType() == this.getType();
		} else {
			return false;
		}
	}

	@Override
	public List<AST> findAll(AST tree) {
		return null;
	}

	@Override
	public AST getNthChild(int n) {
		return this.children.get(n);
	}

	@Override
	public void setNthChild(int n, AST node) {
		this.children.add(n, node);
	}

	@Override
	public void removeAllChildren() {
		this.children.clear();
	}

	@Override
	public void removeNthChild(int n) {
		this.children.remove(n);
	}

	@Override
	public int getNumberOfChildren() {
		return this.children.size();
	}
}
