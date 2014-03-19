package com.dw.memdb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

public class MemDbParser {

	public Message parseColumnDefinition(String columnsDefinition,
			HashMap<String, Column> columns) {

		if (columnsDefinition.equals("") || columnsDefinition.length() == 0)
			return new Message("Table with zero columns cannot be created",
					true);

		String[] definitions = columnsDefinition.split(",");

		Message m = null;

		for (int i = 0; i < definitions.length; i++) {

			Column column = new Column(i);
			m = parseColumnDefinition(definitions[i].trim(), column);

			if (!m.error) {

				if (!columns.containsKey(column.header)) {
					columns.put(column.header, column);
				} else {
					columns = null;
					return new Message(
							"Duplicate columns found. Table creation failed.",
							true);
				}

			} else {
				columns = null;
				return m;
			}

		}

		return new Message("Columns successfully created", false);

	}

	private Message parseColumnDefinition(String columnDefinition, Column column) {

		if (columnDefinition.equals("") || columnDefinition.length() == 0)
			return new Message("Invalid column definition", true);

		String[] columnDefinitionTokens = columnDefinition.split(" ");

		return (createColumnFromTokens(columnDefinitionTokens, column));

	}

	private Message createColumnFromTokens(String[] columnDefinitionTokens,
			Column column) {

		if (columnDefinitionTokens.length <= 1
				|| columnDefinitionTokens.length > 2) // check only two tokens
														// allowed for now
			return new Message("Invalid column definition", true);

		if (columnDefinitionTokens[0].length() == 0
				|| columnDefinitionTokens[0].equals(""))
			return new Message("Invalid column definition", true);

		for (int i = 1; i < columnDefinitionTokens.length; i++) {

			switch (columnDefinitionTokens[i]) {
			case "INTEGER":
			case "INT":
				initializeColumn(columnDefinitionTokens[0],
						DataType.integerType, -1, column);
				break;
			case "BOOLEAN":
			case "BOOL":
				initializeColumn(columnDefinitionTokens[0],
						DataType.booleanType, -1, column);
				break;

			default:
				if (columnDefinitionTokens[i].contains("(")
						&& columnDefinitionTokens[i].contains(")")
						&& (columnDefinitionTokens[i].indexOf("(") + 1) < columnDefinitionTokens[i]
								.indexOf(")")) {
					if (columnDefinitionTokens[i].contains("VARCHAR")) {
						try {
							int dataTypeSize = Integer
									.parseInt(columnDefinitionTokens[i]
											.substring(
													columnDefinitionTokens[i]
															.indexOf("(") + 1,
													columnDefinitionTokens[i]
															.indexOf(")")));
							if (dataTypeSize <= 0) {
								return (new Message(
										"Invalid column datatype size", true));
							}
							initializeColumn(columnDefinitionTokens[0],
									DataType.varcharType, dataTypeSize, column);
						} catch (Exception e) {
							return (new Message("Invalid column definition",
									true));
						}
					} else if (columnDefinitionTokens[i].contains("CHAR")) {
						try {
							int dataTypeSize = Integer
									.parseInt(columnDefinitionTokens[i]
											.substring(
													columnDefinitionTokens[i]
															.indexOf("(") + 1,
													columnDefinitionTokens[i]
															.indexOf(")")));
							if (dataTypeSize <= 0) {
								return (new Message(
										"Invalid column datatype size", true));
							}
							initializeColumn(columnDefinitionTokens[0],
									DataType.charType, dataTypeSize, column);
						} catch (Exception e) {
							return (new Message("Invalid column definition",
									true));
						}
					}
				} else
					return (new Message("Invalid column definition", true));
				break;
			}
		}

		return new Message("Column created successfully.", false);
	}

	private void initializeColumn(String header, int datatype, int size,
			Column column) {

		column.initialize(header, datatype, size);
	}

	public Message addRow(Table table, String values) {

		if (values.contains("values(")
				&& values.contains(")")
				&& (values.indexOf("values(") + "values(".length()) < values
						.indexOf(")")) {

			values = values.substring(
					values.indexOf("values(") + "values(".length(),
					values.indexOf(")"));

			// System.out.println("values:" + values);

			String[] tuple = values.split(",");

			if (tuple.length == table.columns.size()) {

				Column column = null;

				Iterator<Column> iterator = table.columns.values().iterator();

				int index = 0;

				Vector<Column> columnsProcessed = new Vector<>();

				while (iterator.hasNext()) {

					column = iterator.next();

					Value v = null;

					switch (column.dataType) {

					case DataType.integerType:
						try {
							v = new IntegerValue(tuple[column.order]);
							columnsProcessed.add(column);
						} catch (NumberFormatException ex) {
							rollbackColumnsProcessed(columnsProcessed);
							return new Message("Invalid row values", true);
						}

						break;

					case DataType.charType:
						if (tuple[column.order].indexOf("'") == 0
								&& (tuple[column.order].lastIndexOf("'") == (tuple[column.order]
										.length()) - 1)
								&& (tuple[column.order].length() - 2) <= column.dataSize) {
							v = new CharValue(tuple[column.order].substring(1,
									(tuple[column.order].length() - 1))
									.toCharArray(), column.dataSize);
							columnsProcessed.add(column);

						} else {
							rollbackColumnsProcessed(columnsProcessed);
							return new Message("Invalid row values", true);
						}
						break;

					case DataType.varcharType:
						if (tuple[column.order].indexOf("'") == 0
								&& (tuple[column.order].lastIndexOf("'") == (tuple[column.order]
										.length()) - 1)
								&& (tuple[column.order].length() - 2) <= column.dataSize) {
							v = new VarcharValue(tuple[column.order].substring(
									1, (tuple[column.order].length() - 1)));
							columnsProcessed.add(column);
						} else {
							rollbackColumnsProcessed(columnsProcessed);
							return new Message("Invalid row values", true);
						}

						break;

					}

					column.values.add(v);
				}

				columnsProcessed = null;

			} else {
				return new Message("Not sufficient values", true);
			}
		} else {
			return new Message("Invalid Syntax", true);
		}
		return new Message("Row successfully created", false);
	}

	private Message rollbackColumnsProcessed(Vector<Column> columnsProcessed) {

		Iterator<Column> iterator = columnsProcessed.iterator();
		Column column = null;
		while (iterator.hasNext()) {
			column = iterator.next();

			if (column.values.size() >= 1) {

				column.values.remove(column.values.size() - 1);
			}
		}
		return new Message("Processed column data successfully rollbacked",
				false);
	}

	public Message selectTable(Table table, String columnNames,
			String filterCondition) {
		String result = "";

		if (filterCondition != null) {
			// evaluate filter condition here to get rowIndexs...

		} else { // otherwise do full table scan for input columns!

			String[] columnNamesArray = columnNames.split(",");

			Column[] columns = new Column[columnNamesArray.length];

			for (int i = 0; i < columnNamesArray.length; i++) {

				columns[i] = table.columns.get(columnNamesArray[i].trim());

				result += columns[i].header + " | ";
			}

			result += "\n";
			// perform full table scan ...
			int index = 0;
			int countRows = columns[0].values.size();

			while (index < countRows) {
				for (int i = 0; i < columnNamesArray.length; i++) {

					result += getValueByColumnAndRowIndex(columns[i], index);

					result += " | ";
				}

				result += "\n";
				index++;
			}

			return (new Message(countRows + " rows found", result, false));
		}

		return null;

	}

	private String getValueByColumnAndRowIndex(Column column, int rowIndex) {

		if (rowIndex >= 0) {
			Value v = column.values.get(rowIndex);

			if (v instanceof IntegerValue) {
				return (((IntegerValue) v).toString());

			}

			else if (v instanceof VarcharValue) {
				return (((VarcharValue) v).toString());

			}

			else if (v instanceof CharValue) {
				return (((CharValue) v).toString());
			}
		}
		return null;
	}

	public Message calculateAggregates(Table table, String columnNames) {

		String result = "";
		int aggregateResult = 0;
		String aggregateIdentifier = columnNames.substring(0,
				columnNames.indexOf("("));
		String columnName = columnNames.substring(columnNames.indexOf("(") + 1,
				columnNames.indexOf(")"));
		Column column = table.columns.get(columnName);

		switch (aggregateIdentifier) {

		case AggregateType.SUM: {
			if (column.dataType == DataType.integerType) {
				result += "SUM(" + column.header + ")" + "\n";
				for (int i = 0; i < column.values.size(); i++) {
					aggregateResult += ((IntegerValue) column.values.get(i)).value;
				}
			} else {
				return new Message("Aggregate on invalid column type", true);
			}
		}
		}

		System.out.print(result);
		for(int i=0;i<result.length();i++){System.out.print("-");}
		System.out.println();
		System.out.println(aggregateResult);
		return new Message("Aggregate calculated successfully.",false);
	}
}
