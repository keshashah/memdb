package com.dw.memdb;

public class VarcharValue extends Value {

	char[] value;
	public VarcharValue(String value) {
		this.value = value.toCharArray();
	}
	
	public String toString()
	{
		return new String(value);
	}
}

