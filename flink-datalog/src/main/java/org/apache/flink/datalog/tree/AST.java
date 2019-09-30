package org.apache.flink.datalog.tree;

import org.antlr.v4.runtime.Token;

import java.util.List;

public interface AST {

	void addChild(AST c);

	boolean equals(AST t);

	List<AST> findAll(AST tree);

	AST getFirstChild();

	void initialize(int t, String txt);

	void initialize(AST t);

	void initialize(Token t);

	void setFirstChild(AST c);

	AST getNextSibling();

	void setNextSibling(AST n);

	String getText();

	void setText(String text);

	NodeType getType();

	void setType(NodeType typ);

	int getNumberOfChildren();
}
