package com.stackroute.datamunger.query.parser;

import java.util.List;
import java.util.ArrayList;

/* 
 * This class will contain the elements of the parsed Query String such as conditions,
 * logical operators,aggregate functions, file name, fields group by fields, order by
 * fields, Query Type
 * */

public class QueryParameter {
	private String fileName;
	private String baseQuery;
	private List<Restriction> restrictions = new ArrayList<Restriction>();
	private List<String> logicalOperators = new  ArrayList<String>();
	private List<String> fields = new ArrayList<String>();
	private List<AggregateFunction> aggregateFunctions = new ArrayList<AggregateFunction>();	
	private List<String> groupByFields = new ArrayList<String>();
	private List<String> orderByFields = new ArrayList<String>();	

	public String getFileName() {
		return fileName;
	}
	
	public void setFileName(final String file) {
		this.fileName = file;
	}

	public String getBaseQuery() {
		return baseQuery;
	}
	
	public void setBaseQuery(final String baseQuery) {
		this.baseQuery = baseQuery;
	}

	public List<Restriction> getRestrictions() {
		return restrictions;
	}
	
	public void setRestrictions(final List<Restriction> restrictions) {
		this.restrictions = restrictions;
	}

	public List<String> getLogicalOperators() {
		return logicalOperators;
	}
	
	public void setLogicalOperators(final List<String> logicalOperators) {
		this.logicalOperators = logicalOperators;
	}

	public List<String> getFields() {
		return fields;
	}
	
	public void setFields(final List<String> fields) {
		this.fields = fields;
	}

	public List<AggregateFunction> getAggregateFunctions() {
		return aggregateFunctions;
	}
	
	public void setAggregateFunctions(final List<AggregateFunction> aggregateFunctions) {
		this.aggregateFunctions = aggregateFunctions;
	}

	public List<String> getGroupByFields() {
		return groupByFields;
	}
	
	public void setGroupByFields(final List<String> groupFields) {
		this.groupByFields = groupFields;
	}

	public List<String> getOrderByFields() {
		return orderByFields;
	}
	
	public void setOrderByFields(final List<String> orderByFields) {
		this.orderByFields = orderByFields;
	}
}