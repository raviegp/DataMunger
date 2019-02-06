package com.stackroute.datamunger.query.parser;

import java.util.ArrayList;
import java.util.List;

/* 
 * This class will contain the elements of the parsed Query String such as conditions,
 * logical operators,aggregate functions, file name, fields group by fields, order by
 * fields, Query Type
 * */
public class QueryParameter {

	private String fileName;
	private String baseQuery;
	private List<Restriction> restrictions = new ArrayList<Restriction>();
	private List<String> fields = new ArrayList<String>();
	private List<String> logicalOperators = new ArrayList<String>();
	private List<String> orderByFields = new ArrayList<String>();
	private List<String> groupByFields = new ArrayList<String>();
	private List<AggregateFunction> aggregateFunctions = new ArrayList<AggregateFunction>();
	private String QUERY_TYPE;

	public String getFileName() {
		// TODO Auto-generated method stub
		return fileName;
	}

	public void setFileName(final String file) {
		this.fileName = file;
	}

	public String getBaseQuery() {
		// TODO Auto-generated method stub
		return baseQuery;
	}

	public void setBaseQuery(final String baseQuery) {
		this.baseQuery = baseQuery;
	}

	public List<Restriction> getRestrictions() {
		// TODO Auto-generated method stub
		return restrictions;
	}

	public void setRestrictions(final List<Restriction> restrictions) {
		this.restrictions = restrictions;
	}

	public List<String> getFields() {
		// TODO Auto-generated method stub
		return fields;
	}

	public void setFields(final List<String> fields) {
		this.fields = fields;
	}

	public List<AggregateFunction> getAggregateFunctions() {
		// TODO Auto-generated method stub
		return aggregateFunctions;
	}

	public void setAggregateFunctions(final List<AggregateFunction> aggregateFunctions) {
		this.aggregateFunctions = aggregateFunctions;
	}

	public List<String> getLogicalOperators() {
		// TODO Auto-generated method stub
		return logicalOperators;
	}

	public void setLogicalOperators(final List<String> logicalOperators) {
		this.logicalOperators = logicalOperators;
	}

	public List<String> getGroupByFields() {
		// TODO Auto-generated method stub
		return groupByFields;
	}

	public void setGroupByFields(final List<String> groupFields) {
		this.groupByFields = groupFields;
	}

	public List<String> getOrderByFields() {
		// TODO Auto-generated method stub
		return orderByFields;
	}

	public void setOrderByFields(final List<String> orderByFields) {
		this.orderByFields = orderByFields;
	}

	public String getQUERY_TYPE() {
		// TODO Auto-generated method stub
		return QUERY_TYPE;
	}

	public void setQUERY_TYPE(final String QUERY_TYPE) {
		this.QUERY_TYPE = QUERY_TYPE;
	}

}
