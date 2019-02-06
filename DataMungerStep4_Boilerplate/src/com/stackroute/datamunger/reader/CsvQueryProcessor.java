package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {
	
	private String fileName;
	private String[] dataRow = null;

	/*
	 * Parameterized constructor to initialize filename. As you are trying to
	 * perform file reading, hence you need to be ready to handle the IO Exceptions.
	 */
	
	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		this.fileName = fileName;
		if (!new File(fileName).exists()) {
			throw new FileNotFoundException();
		}
	}

	/*
	 * Implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file.
	 */

	@Override
	public Header getHeader() throws IOException {
		
		String firstLine = null;
		String headerArgs[] = null;
		String strCurrentLine = null;
		Header header = null;
		
		// populate the header object with the String array containing the header names
		try (BufferedReader br = new BufferedReader(new FileReader(this.fileName))) {
			if ((strCurrentLine = br.readLine()) != null) {
				// read the first line
				firstLine = strCurrentLine;
				headerArgs = firstLine.split("[,]+");
				header = new Header(headerArgs);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return header;
	}

	/**
	 * This method will be used in the upcoming assignments
	 */
	@Override
	public void getDataRow() {

	}

	/*
	 * Implementation of getColumnType() method. To find out the data types, we will
	 * read the first line from the file and extract the field values from it. In
	 * the previous assignment, we have tried to convert a specific field value to
	 * Integer or Double. However, in this assignment, we are going to use Regular
	 * Expression to find the appropriate data type of a field. Integers: should
	 * contain only digits without decimal point Double: should contain digits as
	 * well as decimal point Date: Dates can be written in many formats in the CSV
	 * file. However, in this assignment,we will test for the following date
	 * formats('dd/mm/yyyy',
	 * 'mm/dd/yyyy','dd-mon-yy','dd-mon-yyyy','dd-month-yy','dd-month-yyyy','yyyy-mm
	 * -dd')
	 */
	
	@Override
	public DataTypeDefinitions getColumnType() throws IOException {
		
		// checking for Integer

		// checking for floating point numbers

		// checking for date format dd/mm/yyyy

		// checking for date format mm/dd/yyyy

		// checking for date format dd-mon-yy

		// checking for date format dd-mon-yyyy

		// checking for date format dd-month-yy

		// checking for date format dd-month-yyyy

		// checking for date format yyyy-mm-dd

		DataTypeDefinitions dataTypeDefinitions = null;
		String dataTypes[] = null;
		String numbersRegEx = "\\d+";
		String floatRegEx = "^([+-]?\\d*\\.?\\d*)$";

		try (BufferedReader br = new BufferedReader(new FileReader(this.fileName))) {
			// read the second line
			 br.readLine();
			 dataRow = br.readLine().split("[,]+", 18);
			
			if (dataRow.length != 0) {
				dataTypes = new String[dataRow.length];
				for (int i = 0; i < dataRow.length; i++) {
					if (dataRow[i].matches(numbersRegEx)) {
						dataTypes[i] = Integer.class.getName();
					} else if (dataRow[i].matches(floatRegEx)) {
						dataTypes[i] = Object.class.getName();
					} else if (dataRow[i].matches("\\d{2}[/|\\|-]\\d{2}[/|\\|-]\\d{4}")) {
						// checking for date format dd/mm/yyyy or dd\mm\yyyy or dd-mm-yyyy or mm/dd/yyyy or mm\dd\yyyy or mm-dd-yyyy
						dataTypes[i] = Date.class.getName();
					} else if (dataRow[i].matches("\\d{2}[/|\\|-][a-z]{3}[/|\\|-]\\d{2}")) {
						// checking for date format dd/mon/yy or dd\mon\yy or dd-mon-yy or yy/mon/dd or yy\mon\dd or yy-mon-dd
						dataTypes[i] = Date.class.getName();
					} else if (dataRow[i].matches("\\d{2}[/|\\|-][a-z]{3}[/|\\|-]\\d{4}")) {
						// checking for date format dd/mon/yyyy or dd\mon\yyyy or dd-mon-yyyy
						dataTypes[i] = Date.class.getName();
					} else if (dataRow[i].matches("\\d{2}[/|\\|-][a-z]{3}[/|\\|-]\\d{2}")) {
						// checking for date format dd/mon/yy or dd\mon\yy or dd-mon-yy or yy/mon/dd or yy\mon\dd or yy-mon-dd
						dataTypes[i] = Date.class.getName();
					} else if (dataRow[i].matches("\\d{2}[/|\\|-][a-z]{5}[/|\\|-]\\d{4}")) {
						// checking for date format dd/month/yyyy or dd\month\yyyy or dd-month-yyyy
						dataTypes[i] = Date.class.getName();
					} else if (dataRow[i].matches("\\d{4}[/|\\|-]\\d{2}[/|\\|-]\\d{2}")) {
						// checking for date format yyyy/mm/dd or yyyy\mm\dd or yyyy-mm-dd or yyyy/dd/mm or yyyy\dd\mm or yyyy-dd-mm
						dataTypes[i] = Date.class.getName();
					} else {
						dataTypes[i] = String.class.getName();
					}
				}
			}

			dataTypeDefinitions = new DataTypeDefinitions(dataTypes);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return dataTypeDefinitions;
	}

}
