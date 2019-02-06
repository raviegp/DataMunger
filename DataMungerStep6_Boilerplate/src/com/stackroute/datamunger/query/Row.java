package com.stackroute.datamunger.query;

import java.util.HashMap;
import java.util.Map;

//contains the row object as ColumnName/Value. Hence, HashMap is being used

public class Row extends HashMap<String, String>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Map<String, String> rowMap;

	public Row() {

	}

	public Row(final Map<String, String> rowMap) {
		this.rowMap = rowMap;
	}

	public Map<String, String> getRow() {
		return rowMap;
	}

	public void setRow(final Map<String, String> rowMap) {
		this.rowMap = rowMap;
	}
}
