package com.stackroute.datamunger.query;

import java.util.LinkedHashMap;
import java.util.Map;

//This class will be acting as the DataSet containing multiple rows
public class DataSet extends LinkedHashMap<Long, Row> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Map<Long, Row> dataSetMap;

	public DataSet() {

	}

	public DataSet(final Map<Long, Row> dataSetMap) {
		this.dataSetMap = dataSetMap;
	}

	public Map<Long, Row> getDataSet() {
		return dataSetMap;
	}

	public void setDataSet(final Map<Long, Row> dataSetMap) {
		this.dataSetMap = dataSetMap;
	}

}
