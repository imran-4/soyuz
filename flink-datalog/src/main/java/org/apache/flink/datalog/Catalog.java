package org.apache.flink.datalog;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
* Stores Tables related information in a map to lookup during creating logical plans.
*/

class Catalog implements Serializable {
	private Map<String, RelationInfo> relationsLookup = new HashMap<>();

	RelationInfo getRelationInfo(String name) {
		return this.relationsLookup.getOrDefault(name, null);
	}

	void addOrUpdateRelation(String name, RelationInfo info) {
		this.relationsLookup.put(name, info); //update if already existed, otherwise add it
	}

	void addOrUpdateRelation(String name, List<String> schema) {
		RelationInfo info = new RelationInfo();
		this.addOrUpdateRelation(name, info);
	}

	void removeRelation(String name) {
		this.relationsLookup.remove(name);
	}

	void removeAll() {
		this.relationsLookup.clear();
	}
}
