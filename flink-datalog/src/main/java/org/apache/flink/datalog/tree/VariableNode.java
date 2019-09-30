package org.apache.flink.datalog.tree;

import org.antlr.v4.runtime.Token;

import java.util.List;

public class VariableNode extends AbstractAST {


	@Override
	public void addChild(AST c) {

	}

	@Override
	public boolean equals(AST t) {
		return false;
	}

	@Override
	public List<AST> findAll(AST tree) {
		return null;
	}

	@Override
	public AST getFirstChild() {
		return null;
	}

	@Override
	public void initialize(int t, String txt) {

	}

	@Override
	public void initialize(AST t) {

	}

	@Override
	public void initialize(Token t) {

	}

	@Override
	public void setFirstChild(AST c) {

	}

	@Override
	public AST getNextSibling() {
		return null;
	}

	@Override
	public void setNextSibling(AST n) {

	}

	@Override
	public String getText() {
		return null;
	}

	@Override
	public void setText(String text) {

	}

	@Override
	public NodeType getType() {
		return null;
	}

	@Override
	public void setType(NodeType typ) {

	}

	@Override
	public int getNumberOfChildren() {
		return 0;
	}
}
