package org.apache.flink.datalog.tree;

import org.antlr.v4.runtime.Token;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

abstract class AbstractAST implements AST {
	List<AST> children = new ArrayList<>();

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

		return t.getText().equalsIgnoreCase(this.getText()) && t.getType().equals(this.getType());
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

//	@Override
//	public void initialize(int t, String txt) {
//
//	}
//
//	@Override
//	public void initialize(AST t) {
//
//	}
//
//	@Override
//	public void initialize(Token t) {
//
//	}
//
//	@Override
//	public NodeType getType() {
//		return null; //to be implemented by concrete implementations
//	}
//
//	@Override
//	public void setType(NodeType typ) {
//		// //to be implemented by concrete implementations
//	}

	@Override
	public int getNumberOfChildren() {
		return this.children.size();
	}
}
