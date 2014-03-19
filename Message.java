package com.dw.memdb;

public class Message {

	String message = "";
	String output = "";
	boolean error = false;
	
	Message(){}
	
	Message(String message, boolean error)
	{
		super();
		this.message = message;
		this.error = error;
	}
	
	Message(String message, String output, boolean error)
	{
		this(message,error);
		this.output = output;
	}
	
	public String toString()
	{
		if(output!="")
		{
			return "Error:"+error+";;Message:"+message+";;"+"\n"+output;
		}
		else
		{
			return "Error:"+error+";;Message:"+message+";;";	
		}
	}
}
