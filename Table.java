package com.dw.memdb;

import java.util.HashMap;

public class Table implements ITable {

	String header;
	HashMap<String, Column> columns;

	public Table(String tableName, HashMap<String, Column> columns) {
		this.header = tableName;
		this.columns = columns;
	}

	@Override
	public Message createTable(String tableName, HashMap<String, Column> columns) {

		return null;
	}

	@Override
	public Message addColumn(Column column) {
		return null;
	}

	public Message addColumns(Column[] columns) {
		return null;
	}
}
