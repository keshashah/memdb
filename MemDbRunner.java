package com.dw.memdb;

import com.sun.jmx.snmp.Timestamp;

public class MemDbRunner {

	public static void main(String args[]){
		
		MEMSqlEngine sqlEngine = new MEMSqlEngine();
		
		//System.out.println(sqlEngine.createDatabase("TESTDB"));
		
		sqlEngine.createDatabase("STUDENTDB");
		
		
		sqlEngine.createTable("STUDENTDB","STUDENT_INFO","ID int, NAME varchar(100), MARKS int, DOB varchar(10)");
		
		//Database database = sqlEngine.getDatabaseByName("TESTDB"); //get database
		//Table table = sqlEngine.getTableByName("TESTDB", "USER");  //get table 
			
		//System.out.println(sqlEngine.createTable("TESTDB","SO_HEADER","ID int, NAME varchar(100), TOTAL_PRICE int, ORDER_DATE varchar(10)"));
		//System.out.println(sqlEngine.createTable("TESTDB","SO_ITEM","ID int, SO_ID int, PR_ID int, QUANTITY int, SALE_PRICE int"));
		//System.out.println(sqlEngine.createTable("TESTDB","SO_PRODUCT","ID int, NAME varchar(20), CURRENT_PRICE int"));
		
		/*System.out.println(sqlEngine.insertStatement("TESTDB","SO_HEADER","values(1,'SAP_LAPTOP_ORDER',20000,'10-02-2013')"));
		System.out.println(sqlEngine.insertStatement("TESTDB","SO_HEADER","values(2,'HP_SERVER_ORDER',80000,'26-04-2013')"));
		*/
		//inserting 1million values
		//Timestamp startTime = new Timestamp(new java.util.Date().getTime());
		/*for(int i=0;i<100000;i++)
		{
			sqlEngine.insertStatement("TESTDB","SO_HEADER","values(1,'HP_SERVER_ORDER',80000,'26-04-2013')");
		}*/
		//Timestamp endTime = new Timestamp(new java.util.Date().getTime());
		//System.out.println(endTime.getDateTime() - startTime.getDateTime());
		/*System.out.println("Records inserted !");
		System.out.println(sqlEngine.insertStatement("TESTDB","SO_ITEM","values(1,1,1,5,12000)"));
		System.out.println(sqlEngine.insertStatement("TESTDB","SO_ITEM","values(1,1,2,2,6000)"));
		System.out.println(sqlEngine.insertStatement("TESTDB","SO_ITEM","values(1,1,3,2,2000)"));
		System.out.println(sqlEngine.insertStatement("TESTDB", "SO_PRODUCT","values(1,'LENOVO_T520',2200)"));
		System.out.println(sqlEngine.insertStatement("TESTDB", "SO_PRODUCT","values(2,'LENOVO_T410',3000)"));
		System.out.println(sqlEngine.insertStatement("TESTDB", "SO_PRODUCT","values(3,'LENOVO_T300',1100)"));
		*/
		
		System.out.println(sqlEngine.insertStatement("studentdb", "student_info", "values(1,'KESHA',99,'11-06-1994')"));
		System.out.println(sqlEngine.insertStatement("studentdb", "student_info", "values(2,'DHWANIT',79,'26-04-1989')"));
		
		//Timestamp startTime = new Timestamp(new java.util.Date().getTime());
		
		//System.out.println(sqlEngine.selectStatement("testdb","so_header","SUM(id)",null));
		
		//Timestamp endTime = new Timestamp(new java.util.Date().getTim:e());
		
		//System.out.println(endTime.getDateTime() - startTime.getDateTime());
		System.out.println(sqlEngine.selectStatement("studentdb","student_info","marks,name",null));
			
		//sqlEngine.selectStatement("testdb","SO_HEADER","*",null);
		//System.out.println(sqlEngine.selectStatement("testdb","SO_header","*",null));
		//System.out.println(sqlEngine.selectStatement("testdb","SO_PRODUCT","*",null));
	}
}