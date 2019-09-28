package org.apache.flink.datalog.tree;

import java.util.List;

public class ConstantNode extends AbstractTreeNode {
	private Object value;

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	@Override
	public List<TreeNode> getChildren() {
		return null;
	}

	@Override
	public List<TreeNode> getSiblings() {
		return null;
	}
}
