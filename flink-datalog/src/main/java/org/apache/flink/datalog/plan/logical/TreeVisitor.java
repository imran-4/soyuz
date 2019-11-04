package org.apache.flink.datalog.plan.logical;

import org.antlr.v4.runtime.tree.RuleNode;
import org.apache.flink.datalog.parser.tree.AndOrTree;
import org.apache.flink.datalog.parser.tree.Node;
import org.apache.flink.datalog.parser.tree.Tree;

public interface TreeVisitor<T> {
	T visit(Tree t);

	T visitChildren(Node node);

}
