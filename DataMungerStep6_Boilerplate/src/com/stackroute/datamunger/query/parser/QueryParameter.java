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
	List<Restriction> restrictions;
	List<String> logicalOperators;
	List<String> fields;
	List<AggregateFunction> aggregateFunctions;
	List<String> groupByFields;
	List<String> orderByFields;

	public final String getFileName() {
		return fileName;
	}

	public final void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public final String getBaseQuery() {
		return baseQuery;
	}

	public final void setBaseQuery(String baseQuery) {
		this.baseQuery = baseQuery;
	}

	public final List<Restriction> getRestrictions() {
		return restrictions;
	}

	public final void setRestrictions(List<Restriction> restrictions) {
		this.restrictions = restrictions;
	}

	public final List<String> getLogicalOperators() {
		return logicalOperators;
	}

	public final void setLogicalOperators(List<String> logicalOperators) {
		this.logicalOperators = logicalOperators;
	}

	public final List<String> getFields() {
		return fields;
	}

	public final void setFields(List<String> fields) {
		this.fields = fields;
	}

	public final List<AggregateFunction> getAggregateFunctions() {
		return aggregateFunctions;
	}

	public final void setAggregateFunctions(List<AggregateFunction> aggregateFunctions) {
		this.aggregateFunctions = aggregateFunctions;
	}

	public final List<String> getGroupByFields() {
		return groupByFields;
	}

	public final void setGroupByFields(List<String> groupByFields) {
		this.groupByFields = groupByFields;
	}

	public final List<String> getOrderByFields() {
		return orderByFields;
	}

	public final void setOrderByFields(List<String> orderByFields) {
		this.orderByFields = orderByFields == null ? new ArrayList<String>() : orderByFields;
	}

	public String getQUERY_TYPE() {
		// TODO Auto-generated method stub
		return null;
	}

}