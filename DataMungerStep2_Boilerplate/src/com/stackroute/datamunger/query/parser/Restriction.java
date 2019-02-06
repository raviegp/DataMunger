package com.stackroute.datamunger.query.parser;

/*
 * This class is used for storing name of field, condition and value for 
 * each conditions
 * generate getter and setter for this class,
 * Also override toString method
 * */

public class Restriction {

	private String propertyName;
	private String propertyValue;
	private String condition;

	// Write logic for constructor
	public Restriction(final String name, final String value, final String condition) {
		this.propertyName = name;
		this.propertyValue = value;
		this.condition = condition;
	}

	public String getPropertyName() {
		return propertyName;
	}

	public String getPropertyValue() {
		return propertyValue;
	}

	public String getCondition() {
		return condition;
	}

	public void setPropertyName(final String name) {
		this.propertyName = name;
	}

	public void setPropertyValue(final String value) {
		this.propertyValue = value;
	}

	public void setCondition(final String condition) {
		this.condition = condition;
	}

	public String toString() {
		return propertyName + " " + propertyValue + " " + condition;
	}

}