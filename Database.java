package com.dw.memdb;

import java.util.HashMap;

public class Database implements IDatabase{

	String dbName;
	HashMap<String, Table> tables;

	public Database(String dbName)
	{
		this.dbName = dbName;
		tables = new HashMap<>(); 
	}
}
