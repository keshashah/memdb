package com.dw.memdb;

public class CharValue extends Value {

	char[] value;

	public CharValue(char[] value,int dataSize) {
		
		this.value = new char[dataSize];
		
		for(int i=0;i<value.length;i++)
		{
			this.value[i] = value[i];
		}
	}
	
	public String toString()
	{
		return new String(value);
	}
	
}
