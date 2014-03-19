package com.dw.memdb;

import java.util.ArrayList;
import java.util.List;

public class Column implements IColumn {

	int currentFreePosition = 0;
	String header;
	int dataType;
	int dataSize;
	int order;
	String constraints;
	ArrayList<Value> values;
	
	public Column(int order) {
		this.order = order;
	}
	
	public void initialize(String header, int dataType, int dataSize) {
		
		this.header = header;
		values = new ArrayList<Value>();
		
		switch (dataType) {
		
		case DataType.integerType:
			this.dataType = DataType.integerType;
			this.dataSize = -1; 
			break;
			
		case DataType.charType:
			this.dataType= DataType.charType;
			this.dataSize = dataSize;
			break;
			
		case DataType.varcharType:
			this.dataType = DataType.varcharType;
			this.dataSize = dataSize;
			break;
		}

	}
}
