package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {

	private String fileName;
	private String[] dataRow = null;

	// Parameterized constructor to initialize filename
	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		this.fileName = fileName;
		if (!new File(fileName).exists()) {
			throw new FileNotFoundException();
		}
	}

	/*
	 * Implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file. Note: Return type of the method will be
	 * Header
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
	 * getDataRow() method will be used in the upcoming assignments
	 */

	@Override
	public void getDataRow() {

	}

	/*
	 * Implementation of getColumnType() method. To find out the data types, we will
	 * read the first line from the file and extract the field values from it. If a
	 * specific field value can be converted to Integer, the data type of that field
	 * will contain "java.lang.Integer", otherwise if it can be converted to Double,
	 * then the data type of that field will contain "java.lang.Double", otherwise,
	 * the field is to be treated as String. Note: Return Type of the method will be
	 * DataTypeDefinitions
	 */

	@Override
	public DataTypeDefinitions getColumnType() throws IOException {

		DataTypeDefinitions dataTypeDefinitions = null;
		String dataTypes[] = null;
		String numbersRegEx = "\\d+";

		try (BufferedReader br = new BufferedReader(new FileReader(this.fileName))) {
			// read the second line
			 br.readLine();
			 dataRow = br.readLine().split("[,]+", 18);
			
			if (dataRow.length != 0) {
				dataTypes = new String[dataRow.length];
				for (int i = 0; i < dataRow.length; i++) {
					if (dataRow[i].matches(numbersRegEx)) {
						dataTypes[i] = Integer.class.getName();
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
