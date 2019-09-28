package org.apache.flink.datalog.tree;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractTreeNode implements TreeNode {

	private String nodeName;

	@Override
	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	@Override
	public String getNodeName() {
		return this.nodeName;
	}

	public abstract List<TreeNode> getChildren();

	public abstract List<TreeNode> getSiblings();
}
