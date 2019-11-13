package org.apache.flink.datalog.parser.tree;

import org.antlr.v4.runtime.tree.RuleNode;

public abstract class AbstractTreeVisitor<T> implements TreeVisitor<T> {
	public AbstractTreeVisitor() {
	}

	@Override
	public void visit(Tree tree) {
		tree.accept(this);
	}

	@Override
	public void visitChildren(Node node) {
		T result = this.defaultResult();
		for(int i = 0; i < node.getChildCount(); i++) {
			Node c = node.getChild(i);
			c.accept(this);
		}
	}

	protected T defaultResult() {
		return null;
	}
}
