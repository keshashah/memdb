package com.dw.memdb;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;

public class MEMSqlEngine {

	HashMap<String, Database> databases;

	public MEMSqlEngine() {
		databases = new HashMap<>();
	}

	public Message createDatabase(String dbName) {
		dbName = MemDbUtils.convertToUpperCase(MemDbUtils.trimString(dbName));
		if (!databases.containsKey(dbName)) {
			Database db = new Database(dbName);
			databases.put(dbName, db);
			return (new Message("Database successfully created", false));
		} else {
			return (new Message("Database already exists", true));
		}
	}

	private Database getDatabaseByName(String dbName) {
		return databases.get(dbName.toUpperCase());
	}

	private Table getTableByName(String dbName, String tableName) {
		Database database = getDatabaseByName(dbName);
		return database.tables.get(tableName.toUpperCase());
	}

	public Message createTable(String dbName, String tableName,
			String columnsDefinition) {

		dbName = MemDbUtils.convertToUpperCase(dbName);
		tableName = MemDbUtils.convertToUpperCase(tableName);
		columnsDefinition = MemDbUtils.convertToUpperCase(columnsDefinition);

		Database database = getDatabaseByName(dbName);

		if (database != null) {
			if (database.tables != null) {
				if (database.tables.containsKey(tableName))
					return new Message(
							"Table already exists. Table creation failed", true);
			}

			HashMap<String, Column> columns = new HashMap<>();
			Message m = (new MemDbParser()).parseColumnDefinition(
					columnsDefinition.trim(), columns);
			if (!m.error) {
				Table table = new Table(tableName, columns);
				if (database.tables == null) {
					database.tables = new HashMap<>();
				}
				database.tables.put(tableName, table);
			} else {
				return m;
			}
		} else {
			return new Message("No Database found. Table creation failed", true);
		}
		return (new Message("Table successfully created", false));
	}

	public Message insertStatement(String dbName, String tableName,
			String values) {

		dbName = MemDbUtils.convertToUpperCase(dbName);
		tableName = MemDbUtils.convertToUpperCase(tableName);

		Database database = getDatabaseByName(dbName);

		if (database != null) {
			if (database.tables != null) {
				Table table = database.tables.get(tableName);
				if (table != null) {
					return ((new MemDbParser()).addRow(table, values));

				} else {
					return new Message("No such table exists", true);
				}

			} else {
				return new Message("No such table exists", true);
			}
		} else {
			return new Message("No Database found. Table creation failed", true);
		}
	}

	public Message selectStatement(String dbName, String tableName,
			String columnNames, String filterCondition) {

		//Timestamp startTime = new Timestamp(new java.util.Date().getTime());

		dbName = MemDbUtils.convertToUpperCase(MemDbUtils.trimString(dbName));
		tableName = MemDbUtils.convertToUpperCase(MemDbUtils
				.trimString(tableName));
		columnNames = MemDbUtils.convertToUpperCase(MemDbUtils
				.trimString(columnNames));

		Database database = getDatabaseByName(dbName);

		if (database != null) {
			if (database.tables != null) {
				Table table = database.tables.get(tableName);
				if (table != null) {

					Message m = checkForAggregateQuery(table, columnNames);

					if (!m.error) {
						m = (new MemDbParser()).calculateAggregates(table,
								columnNames);
						return m;
					}

					m = validateColumnNamesForTable(table, columnNames);

					if (!m.error) {

						if (columnNames.equals("*")) {
							columnNames = "";
							Iterator<String> iterator = table.columns.keySet()
									.iterator();
							while (iterator.hasNext()) {
								columnNames += iterator.next() + ",";
							}
							columnNames = columnNames.substring(0,
									columnNames.length() - 1);
						}

						m = (new MemDbParser()).selectTable(table, columnNames,
								filterCondition);

						/*Timestamp endTime = new Timestamp(
								new java.util.Date().getTime());
						System.out.println(endTime.getTime()
								- startTime.getTime());*/
						return m;

					} else {
						return m;
					}

				} else {
					return new Message("No such table exists", true);
				}

			} else {
				return new Message("No such table exists", true);
			}
		} else {
			return new Message("No Database found. Table creation failed", true);
		}
	}

	private Message checkForAggregateQuery(Table table, String columnNames) {

		if (columnNames.contains("(") && columnNames.contains(")")) {
			String aggregateIdentifier = columnNames.substring(0,
					columnNames.indexOf("("));
			String columnName = columnNames.substring(
					columnNames.indexOf("(") + 1, columnNames.indexOf(")"));

			switch (aggregateIdentifier) {
			case AggregateType.SUM:
			case AggregateType.COUNT:
			case AggregateType.MAX:
			case AggregateType.MIN:
			case AggregateType.AVG:
				Message m = validateColumnNamesForTable(table, columnName);
				if (!m.error)
					return new Message("Aggregate column detected", false);
				else
					return m;
			default:
				return new Message("Invalid Aggregate Field", true);
			}
		}
		return new Message("No Aggregates found", true);
	}

	private Message validateColumnNamesForTable(Table table, String columnNames) {
		if (!columnNames.equals("*")) {
			String[] columns = columnNames.split(",");
			if (columns.length != 0) {
				if (columns[0].trim().length() > 0) {
					for (int i = 0; i < columns.length; i++) {
						if (!table.columns.containsKey(columns[i].trim())) {
							return new Message("Invalid Column Names", true);
						}
					}
					return new Message("Valid Column Names", false);

				} else {
					return new Message("Invalid Column Names", true);
				}
			} else {
				return new Message("Invalid Column Names", true);
			}
		} else {
			return new Message("Valid Column Names", false);
		}
	}

}
