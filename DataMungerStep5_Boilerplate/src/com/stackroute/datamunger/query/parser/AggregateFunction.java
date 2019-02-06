package com.stackroute.datamunger.query.parser;

/* This class is used for storing name of field, aggregate function for 
 * each aggregate function
 * */
public class AggregateFunction {

	private String field;
	private String function;

	public AggregateFunction() {

	}

	public AggregateFunction(final String field, final String function) {
		this.field = field;
		this.function = function;
	}

	public String getFunction() {
		// TODO Auto-generated method stub
		return function;
	}

	public void setFunction(final String function) {
		this.function = function;
	}

	public String getField() {
		// TODO Auto-generated method stub
		return field;
	}

	public void setField(final String field) {
		this.field = field;
	}

	public String toString() {
		return field + " " + function;
	}

}
