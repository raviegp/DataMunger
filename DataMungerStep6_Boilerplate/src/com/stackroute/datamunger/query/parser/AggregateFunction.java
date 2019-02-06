package com.stackroute.datamunger.query.parser;

/* This class is used for storing name of field, aggregate function for 
 * each aggregate function
 * */
public class AggregateFunction {

	private String function, field;

	public AggregateFunction(String field, String function) {
		super();
		this.function = function;
		this.field = field;
	}

	public String getFunction() {
		return this.function;
	}

	public String getField() {
		return this.field;
	}

}