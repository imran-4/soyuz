package org.apache.flink.datalog.tree;

import org.antlr.v4.runtime.Token;

import java.util.List;
import java.util.Stack;

public interface AST {

	void addChild(AST c);

	boolean equals(AST t);

	List<AST> findAll(AST tree);

	AST getNthChild(int n);

	void setNthChild(int n, AST c);

	void removeAllChildren();

	void removeNthChild(int n);

	void initialize(int t, String txt);

	void initialize(AST t);

	void initialize(Token t);

	String getText();

	void setText(String text);

	NodeType getType();

	void setType(NodeType typ);

	int getNumberOfChildren();

	List<AST> getChildren();

}
