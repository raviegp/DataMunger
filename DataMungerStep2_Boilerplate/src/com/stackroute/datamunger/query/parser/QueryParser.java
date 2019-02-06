package com.stackroute.datamunger.query.parser;

import java.util.ArrayList;
import java.util.List;

/*There are total 4 DataMungerTest file:
 * 
 * 1)DataMungerTestTask1.java file is for testing following 4 methods
 * a)getBaseQuery()  b)getFileName()  c)getOrderByClause()  d)getGroupByFields()
 * 
 * Once you implement the above 4 methods,run DataMungerTestTask1.java
 * 
 * 2)DataMungerTestTask2.java file is for testing following 2 methods
 * a)getFields() b) getAggregateFunctions()
 * 
 * Once you implement the above 2 methods,run DataMungerTestTask2.java
 * 
 * 3)DataMungerTestTask3.java file is for testing following 2 methods
 * a)getRestrictions()  b)getLogicalOperators()
 * 
 * Once you implement the above 2 methods,run DataMungerTestTask3.java
 * 
 * Once you implement all the methods run DataMungerTest.java.This test case consist of all
 * the test cases together.
 */

public class QueryParser {

	private QueryParameter queryParameter = new QueryParameter();

	private final String SELECT = "select";
	private final String FROM = "from";
	private final String GROUP = "group";
	private final String ORDER = "order";
	private final String BY = "by";
	private final String WHERE = "where";
	private final String REGW = "[\\W]+";

	/*
	 * This method will parse the queryString and will return the object of
	 * QueryParameter class
	 */
	public QueryParameter parseQuery(String queryString) {

		queryParameter.setBaseQuery(getBaseQuery(queryString));
		queryParameter.setFileName(getFileName(queryString));
		queryParameter.setOrderByFields(getOrderByFields(queryString));
		queryParameter.setGroupByFields(getGroupByFields(queryString));
		queryParameter.setFields(getFields(queryString));
		queryParameter.setRestrictions(getRestrictions(queryString));
		queryParameter.setLogicalOperators(getLogicalOperators(queryString));
		queryParameter.setAggregateFunctions(getAggregateFunctions(queryString));
		
		return queryParameter;
	}

	/*
	 * This method will split the query string based on space into an array of words
	 */
	public String[] getSplitStrings(String queryString) {
		if (null != queryString && !queryString.isEmpty()) {
			return queryString.split(" ");
		}
		return null;
	}

	/*
	 * Extract the name of the file from the query. File name can be found after the
	 * "from" clause.
	 */
	public String getFileName(String queryString) {
		String fileName = "";
		final String[] splitQueryArray = this.getSplitStrings(queryString);

		for (int i = 0; i < splitQueryArray.length; i++) {
			if (splitQueryArray[i].endsWith(".csv")) {
				fileName = splitQueryArray[i];
			}
		}
		return fileName;
	}

	/*
	 * 
	 * Extract the baseQuery from the query.This method is used to extract the
	 * baseQuery from the query string. BaseQuery contains from the beginning of the
	 * query till the where clause
	 */
	public String getBaseQuery(final String queryString) {
		final StringBuffer baseQuery = new StringBuffer();
		final String[] splitQueryArray = this.getSplitStrings(queryString);

		for (int i = 0; i < splitQueryArray.length; i++) {
			if (WHERE.equals(splitQueryArray[i]) || GROUP.equals(splitQueryArray[i])
					|| ORDER.equals(splitQueryArray[i]) && BY.equals(splitQueryArray[i + 1])) {
				break;
			}

			baseQuery.append(splitQueryArray[i]);
			baseQuery.append(' ');
		}
		return baseQuery.toString().trim();
	}

	/*
	 * extract the order by fields from the query string. Please note that we will
	 * need to extract the field(s) after "order by" clause in the query, if at all
	 * the order by clause exists. For eg: select city,winner,team1,team2 from
	 * data/ipl.csv order by city from the query mentioned above, we need to extract
	 * "city". Please note that we can have more than one order by fields.
	 */
	public List<String> getOrderByFields(final String queryString) {
		final StringBuffer orderByFieldsSB = new StringBuffer();
		String[] orderByFieldsArray = null;
		List<String> orderByFieldsList = null;
		final String[] splitQueryArray = this.getSplitStrings(queryString);

		for (int i = 0; i < splitQueryArray.length; i++) {
			if (ORDER.equals(splitQueryArray[i]) && BY.equals(splitQueryArray[i + 1])) {
				for (int j = i + 2; j < splitQueryArray.length; j++) {
					if (splitQueryArray[j].contains(";")) {
						break;
					}
					orderByFieldsSB.append(splitQueryArray[j]);
					orderByFieldsSB.append(' ');
				}

				orderByFieldsArray = orderByFieldsSB.toString().trim().split(" |,");
				orderByFieldsList = new ArrayList<String>();

				for (String orderByFields : orderByFieldsArray) {
					orderByFieldsList.add(orderByFields);
				}
			}
		}
		return orderByFieldsList;
	}

	/*
	 * Extract the group by fields from the query string. Please note that we will
	 * need to extract the field(s) after "group by" clause in the query, if at all
	 * the group by clause exists. For eg: select city,max(win_by_runs) from
	 * data/ipl.csv group by city from the query mentioned above, we need to extract
	 * "city". Please note that we can have more than one group by fields.
	 */
	public List<String> getGroupByFields(final String queryString) {
		final StringBuffer groupByFieldsSB = new StringBuffer();
		String[] groupByFieldsArray = null;
		List<String> groupByFieldsList = null;
		final String[] splitQueryArray = this.getSplitStrings(queryString);

		for (int i = 0; i < splitQueryArray.length; i++) {
			if (GROUP.equals(splitQueryArray[i]) && BY.equals(splitQueryArray[i + 1])) {
				for (int j = i + 2; j < splitQueryArray.length; j++) {
					if (splitQueryArray[j].contains(";")
							|| (splitQueryArray[j].contains(ORDER) && splitQueryArray[j + 1].contains(BY))) {
						break;
					}
					groupByFieldsSB.append(splitQueryArray[j]);
					groupByFieldsSB.append(' ');
				}

				groupByFieldsArray = groupByFieldsSB.toString().trim().split(" |,");
				groupByFieldsList = new ArrayList<String>();

				for (String groupByFields : groupByFieldsArray) {
					groupByFieldsList.add(groupByFields);
				}
			}
		}
		return groupByFieldsList;
	}

	/*
	 * Extract the selected fields from the query string. Please note that we will
	 * need to extract the field(s) after "select" clause followed by a space from
	 * the query string. For eg: select city,win_by_runs from data/ipl.csv from the
	 * query mentioned above, we need to extract "city" and "win_by_runs". Please
	 * note that we might have a field containing name "from_date" or "from_hrs".
	 * Hence, consider this while parsing.
	 */
	public List<String> getFields(final String queryString) {
		final StringBuffer fieldsSB = new StringBuffer();
		String[] fieldsArray = null;
		final String[] splitQueryArray = this.getBaseQuery(queryString).split(" ");
		List<String> fieldsList = new ArrayList<String>();
		
		for (int i = 0; i < splitQueryArray.length; i++) {
			if (splitQueryArray[i].contains(SELECT)) {
				continue;
			} else if (splitQueryArray[i].contains(FROM)) {
				break;
			}
			fieldsSB.append(splitQueryArray[i]);
		}

		fieldsArray = fieldsSB.toString().trim().split(",");

		for (String fields : fieldsArray) {
			fieldsList.add(fields);
		}
		return fieldsList;
	}

	/*
	 * Extract the conditions from the query string(if exists). for each condition,
	 * we need to capture the following: 1. Name of field 2. condition 3. value
	 * 
	 * For eg: select city,winner,team1,team2,player_of_match from data/ipl.csv
	 * where season >= 2008 or toss_decision != bat
	 * 
	 * here, for the first condition, "season>=2008" we need to capture: 1. Name of
	 * field: season 2. condition: >= 3. value: 2008
	 * 
	 * the query might contain multiple conditions separated by OR/AND operators.
	 * Please consider this while parsing the conditions.
	 * 
	 */
	public List<Restriction> getRestrictions(final String queryString) {
		final String[] splitQueryArray = this.getSplitStrings(queryString);
		final StringBuffer restrictionsSB = new StringBuffer();
		List<Restriction> restrictionsList = null;
		String[] restrictionsArray = null;

		if (queryString.contains(" where ")) {
			for (int i = 0; i < splitQueryArray.length; i++) {
				if (splitQueryArray[i].contains(WHERE)) {
					for (int j = i + 1; j < splitQueryArray.length; j++) {
						if (ORDER.equals(splitQueryArray[j])
								|| GROUP.equals(splitQueryArray[j]) && BY.equals(splitQueryArray[j + 1])) {
							break;
						}
						restrictionsSB.append(splitQueryArray[j]);
						restrictionsSB.append(' ');
					}
					break;
				}
			}

			restrictionsArray = restrictionsSB.toString().trim().split(" and | or |;");

			if (restrictionsArray != null) {
				restrictionsList = new ArrayList<Restriction>();
				for (int i = 0; i < restrictionsArray.length; i++) {
					if (restrictionsArray[i].contains(">=")) {
						String[] res = restrictionsArray[i].split(REGW);
						Restriction restriction = new Restriction(res[0].trim(), res[1].trim(), ">=");
						restrictionsList.add(restriction);
					} else if (restrictionsArray[i].contains("<=")) {
						String[] res = restrictionsArray[i].split(REGW);
						Restriction restriction = new Restriction(res[0].trim(), res[1].trim(), "<=");
						restrictionsList.add(restriction);
					} else if (restrictionsArray[i].contains(">")) {
						String[] res = restrictionsArray[i].split(REGW);
						Restriction restriction = new Restriction(res[0].trim(), res[1].trim(), ">");
						restrictionsList.add(restriction);
					} else if (restrictionsArray[i].contains("<")) {
						String[] res = restrictionsArray[i].split(REGW);
						Restriction restriction = new Restriction(res[0].trim(), res[1].trim(), "<");
						restrictionsList.add(restriction);
					}  else if (restrictionsArray[i].contains("!=")) {
						String[] res = restrictionsArray[i].split(REGW);
						Restriction restriction = new Restriction(res[0].trim(), res[1].trim(), "!=");
						restrictionsList.add(restriction);
					} else if (restrictionsArray[i].contains("=")) {
						String[] res = restrictionsArray[i].split(REGW);
						Restriction restriction = new Restriction(res[0].trim(), res[1].trim(), "=");
						restrictionsList.add(restriction);
					}
				}
			}
		}

		return restrictionsList;

	}

	/*
	 * Extract the logical operators(AND/OR) from the query, if at all it is
	 * present. For eg: select city,winner,team1,team2,player_of_match from
	 * data/ipl.csv where season >= 2008 or toss_decision != bat and city =
	 * bangalore
	 * 
	 * The query mentioned above in the example should return a List of Strings
	 * containing [or,and]
	 */
	public List<String> getLogicalOperators(final String queryString) {
		final String[] splitQueryArray = this.getSplitStrings(queryString);
		final StringBuffer logicalOperatorsSB = new StringBuffer();
		String[] logicalOperatorsArray = null;
		List<String> logicalOperatorsList = null;

		if (queryString.toLowerCase().contains(" where ")) {
			for (int i = 0; i < splitQueryArray.length; i++) {
				if (splitQueryArray[i].contains(WHERE)) {
					for (int j = i + 1; j < splitQueryArray.length; j++) {
						if (ORDER.equals(splitQueryArray[j])
								|| GROUP.equals(splitQueryArray[j]) && BY.equals(splitQueryArray[j + 1])) {
							break;
						}
						if ("and".equals(splitQueryArray[j]) || "or".equals(splitQueryArray[j])) {
							logicalOperatorsSB.append(splitQueryArray[j]);
							logicalOperatorsSB.append(' ');
						}
					}
					break;
				}
			}

			logicalOperatorsList = new ArrayList<String>();
			logicalOperatorsArray = logicalOperatorsSB.toString().split(" ");

			for (String logicalOperators : logicalOperatorsArray) {
				logicalOperatorsList.add(logicalOperators);
			}
		}
		return logicalOperatorsList;
	}

	/*
	 * Extract the aggregate functions from the query. The presence of the aggregate
	 * functions can determined if we have either "min" or "max" or "sum" or "count"
	 * or "avg" followed by opening braces"(" after "select" clause in the query
	 * string. in case it is present, then we will have to extract the same. For
	 * each aggregate functions, we need to know the following: 1. type of aggregate
	 * function(min/max/count/sum/avg) 2. field on which the aggregate function is
	 * being applied.
	 * 
	 * Please note that more than one aggregate function can be present in a query.
	 * 
	 * 
	 */
	public List<AggregateFunction> getAggregateFunctions(final String queryString) {
		final StringBuffer aggregateFunctionsSB = new StringBuffer();
		String[] aggregateFunctionsArray = null;
		final String[] splitQueryArray = this.getBaseQuery(queryString).split(" ");
		List<AggregateFunction> aggregateFunctionsList = new ArrayList<AggregateFunction>();

		for (int i = 0; i < splitQueryArray.length; i++) {
			if (splitQueryArray[i].contains(SELECT)) {
				continue;
			} else if (splitQueryArray[i].contains(FROM)) {
				break;
			}
			aggregateFunctionsSB.append(splitQueryArray[i]);
		}
		
		aggregateFunctionsArray = aggregateFunctionsSB.toString().trim().split(",");

		if (!"*".equals(aggregateFunctionsArray[0])) {
			for (int i = 0; i < aggregateFunctionsArray.length; i++) {
				if (aggregateFunctionsArray[i].startsWith("max(") || aggregateFunctionsArray[i].startsWith("min(") || aggregateFunctionsArray[i].startsWith("count(")
						|| aggregateFunctionsArray[i].startsWith("avg(") || aggregateFunctionsArray[i].startsWith("sum") && aggregateFunctionsArray[i].endsWith(")")) {
					String seperator[] = aggregateFunctionsArray[i].split("[)|(]");
					AggregateFunction aggregateFunction = new AggregateFunction(seperator[1], seperator[0]);
					aggregateFunctionsList.add(aggregateFunction);
				}
			}
		}
		return aggregateFunctionsList;
	}

}