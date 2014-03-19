package com.dw.memdb;

import java.util.HashMap;

public interface ITable {

	public Message createTable(String tableName, HashMap<String,Column> columns);
	public Message addColumn(Column column);
	public Message addColumns(Column[] columns);
	
}
