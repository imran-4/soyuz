package org.apache.flink.datalog.parser.tree;

import org.antlr.v4.runtime.tree.RuleNode;
import org.apache.flink.datalog.parser.tree.AndOrTree;
import org.apache.flink.datalog.parser.tree.Node;
import org.apache.flink.datalog.parser.tree.Tree;

public interface TreeVisitor<T> {
	void visit(Tree t);

	void visitChildren(Node node);

}
