package org.apache.flink.datalog.parser.tree;

import org.apache.flink.datalog.parser.tree.AndNode;
import org.apache.flink.datalog.parser.tree.OrNode;

public interface AndOrTreeVisitor<T>  {

	T visitOrNode(OrNode orNode);

	T visitAndNode(AndNode andNode);
}
