package org.apache.flink.datalog.tree;

import java.util.List;

public interface TreeNode {

//	String nodeName = null;
//	List<TreeNode> children = null;
//	List<TreeNode> siblings = null;

	void setNodeName(String nodeName);
	String getNodeName();
	List<TreeNode> getChildren();
	List<TreeNode> getSiblings();
}
