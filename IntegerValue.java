package com.dw.memdb;

public class IntegerValue extends Value {

	int value;

	public IntegerValue(String value) throws NumberFormatException 
	{
		this.value = Integer.parseInt(value);
	}
	
	public String toString()
	{
		return value+"";
	}
}