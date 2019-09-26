package org.apache.flink.datalog;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;

import java.util.Optional;

abstract class DatalogEnvironment implements TableEnvironment {

	private CatalogManager catalogManager;

	abstract Object compile(String text);

	abstract Object query(String queryText); // this should return dataset or datastream

	DatalogEnvironment() {
		catalogManager = new CatalogManager("default", new DatalogCatalog());
	}

	@Override
	public void registerCatalog(String catalogName, Catalog catalog) {
		catalogManager.registerCatalog(catalogName, catalog);

	}

	@Override
	public Optional<Catalog> getCatalog(String catalogName) {
		return catalogManager.getCatalog(catalogName);
	}
}
