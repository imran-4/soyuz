package org.apache.flink.datalog.parser.tree;

import org.apache.flink.datalog.parser.tree.AndNode;
import org.apache.flink.datalog.parser.tree.OrNode;

public interface AndOrTreeVisitor {

	void visitOrNode(OrNode orNode);

	void visitAndNode(AndNode andNode);
}
