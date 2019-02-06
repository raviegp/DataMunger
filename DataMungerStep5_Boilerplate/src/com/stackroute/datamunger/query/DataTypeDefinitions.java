package com.stackroute.datamunger.query;

import java.util.Date;

/*
 * Implementation of DataTypeDefinitions class. This class contains a method getDataTypes() 
 * which will contain the logic for getting the datatype for a given field value. This
 * method will be called from QueryProcessors.   
 * In this assignment, we are going to use Regular Expression to find the 
 * appropriate data type of a field. 
 * Integers: should contain only digits without decimal point 
 * Double: should contain digits as well as decimal point 
 * Date: Dates can be written in many formats in the CSV file. 
 * However, in this assignment,we will test for the following date formats('dd/mm/yyyy',
 * 'mm/dd/yyyy','dd-mon-yy','dd-mon-yyyy','dd-month-yy','dd-month-yyyy','yyyy-mm-dd')
 */
public class DataTypeDefinitions {

	//method stub
	public static Object getDataType(final String input) {
	
		// checking for Integer
		
		// checking for floating point numbers
		
		// checking for date format dd/mm/yyyy
		
		// checking for date format mm/dd/yyyy
		
		// checking for date format dd-mon-yy
		
		// checking for date format dd-mon-yyyy
		
		// checking for date format dd-month-yy
		
		// checking for date format dd-month-yyyy
		
		// checking for date format yyyy-mm-dd
		
		String numbersRegEx = "\\d+";
		String doubleRegEx = "[0-9]{1,13}(\\\\.[0-9]+)?";
		String floatRegEx = "^([+-]?\\d*\\.?\\d*)$";
		
		StringBuffer dataType = new StringBuffer();
		
		if (input != null) {
			if ("".equals(input.trim())) {
				dataType.append((new Object()).getClass().getName());
			} else if (input.matches(numbersRegEx)) {
				dataType.append(Integer.class.getName());
			} else if (input.matches(doubleRegEx)) {
				dataType.append(Double.class.getName());
			} else if (input.matches(floatRegEx)) {
				dataType.append(Float.class.getName());
			} else if (isStringDate(input)) {
				dataType.append(Date.class.getName());
			} else {
				dataType.append(String.class.getName());
			}			
		} else {
			dataType.append((new Object()).getClass().getName());
		}
		
		return dataType.toString();
	}
	
	public static boolean isStringDate(final String input) {
		
		boolean isDateType = false;
		
		if (input.matches("\\d{2}[/|\\|-]\\d{2}[/|\\|-]\\d{4}")) {
			// checking for date format dd/mm/yyyy or dd\mm\yyyy or dd-mm-yyyy or mm/dd/yyyy or mm\dd\yyyy or mm-dd-yyyy
			isDateType = true;
		} else if (input.matches("\\d{2}[/|\\|-][a-z]{3}[/|\\|-]\\d{2}")) {
			// checking for date format dd/mon/yy or dd\mon\yy or dd-mon-yy or yy/mon/dd or yy\mon\dd or yy-mon-dd
			isDateType = true;
		} else if (input.matches("\\d{2}[/|\\|-][a-z]{3}[/|\\|-]\\d{4}")) {
			// checking for date format dd/mon/yyyy or dd\mon\yyyy or dd-mon-yyyy
			isDateType = true;
		} else if (input.matches("\\d{2}[/|\\|-][a-z]{3}[/|\\|-]\\d{2}")) {
			// checking for date format dd/mon/yy or dd\mon\yy or dd-mon-yy or yy/mon/dd or yy\mon\dd or yy-mon-dd
			isDateType = true;
		} else if (input.matches("\\d{2}[/|\\|-][a-z]{5}[/|\\|-]\\d{4}")) {
			// checking for date format dd/month/yyyy or dd\month\yyyy or dd-month-yyyy
			isDateType = true;
		} else if (input.matches("\\d{4}[/|\\|-]\\d{2}[/|\\|-]\\d{2}")) {
			// checking for date format yyyy/mm/dd or yyyy\mm\dd or yyyy-mm-dd or yyyy/dd/mm or yyyy\dd\mm or yyyy-dd-mm
			isDateType = true;
		} 		
		
		return isDateType;
	}
	

	
}
