package com.stackroute.datamunger.query.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class QueryParser {
	private QueryParameter queryParameter = new QueryParameter();

	/*
	 * This method will parse the queryString and will return the object of
	 * QueryParameter class
	 */
	public QueryParameter parseQuery(String queryString) {
		queryParameter.setFileName(getFileName(queryString));
		queryParameter.setBaseQuery(getBaseQuery(queryString));
		queryParameter.setOrderByFields(getOrderByFields(queryString));
		queryParameter.setGroupByFields(getGroupByFields(queryString));
		queryParameter.setFields(getFields(queryString));
		queryParameter.setLogicalOperators(getLogicalOperators(queryString));
		queryParameter.setAggregateFunctions(getAggregateFunctions(queryString));
		queryParameter.setRestrictions(getRestrictions(queryString));
		return queryParameter;
	}

	/*
	 * extract the name of the file from the query. File name can be found after the
	 * "from" clause.
	 */
	private String getFileName(String queryString) {
		if (null != queryString && !queryString.isEmpty()) {
			int indexOf = queryString.indexOf("from ");
			if (indexOf > -1) {
				String substring = queryString.substring(indexOf + 5);
				String tableName = substring.split(" ")[0];
				return tableName;
			}
		}
		return null;
	}

	public String getBaseQuery(String queryString) {
		if (null != queryString && !queryString.isEmpty()) {
			int indexOf = queryString.indexOf("where");
			if (indexOf < 0) {
				indexOf = queryString.indexOf("group by");
			}
			if (indexOf < 0) {
				indexOf = queryString.indexOf("order by");
			}
			if (indexOf > -1) {
				return queryString.substring(0, indexOf - 1);
			} else {
				return queryString;
			}
		}
		return null;
	}

	/*
	 * extract the order by fields from the query string. Please note that we will
	 * need to extract the field(s) after "order by" clause in the query, if at all
	 * the order by clause exists. For eg: select city,winner,team1,team2 from
	 * data/ipl.csv order by city from the query mentioned above, we need to extract
	 * "city". Please note that we can have more than one order by fields.
	 */
	public List<String> getOrderByFields(String queryString) {
		if (null != queryString && !queryString.isEmpty()) {
			queryString = queryString.toLowerCase();
			int startIndex = queryString.indexOf("order by");
			if (startIndex > -1) {
				return Arrays.asList(queryString.substring(startIndex + "order by ".length()).split(" "));
			}
		}
		return null;
	}

	/*
	 * extract the group by fields from the query string. Please note that we will
	 * need to extract the field(s) after "group by" clause in the query, if at all
	 * the group by clause exists. For eg: select city,max(win_by_runs) from
	 * data/ipl.csv group by city from the query mentioned above, we need to extract
	 * "city". Please note that we can have more than one group by fields.
	 */
	public List<String> getGroupByFields(String queryString) {
		if (null != queryString && !queryString.isEmpty()) {
			queryString = queryString.toLowerCase();
			int startIndex = queryString.indexOf("group by ");
			if (startIndex > -1) {
				int endIndex = queryString.indexOf("order by ");
				if (endIndex < 0) {
					return Arrays.asList(queryString.substring(startIndex + "group by ".length()).split(","));
				}
				return Arrays.asList(queryString.substring(startIndex + "group by ".length(), endIndex - 1).split(","));
			}
		}
		return null;
	}

	/*
	 * extract the selected fields from the query string. Please note that we will
	 * need to extract the field(s) after "select" clause followed by a space from
	 * the query string. For eg: select city,win_by_runs from data/ipl.csv from the
	 * query mentioned above, we need to extract "city" and "win_by_runs". Please
	 * note that we might have a field containing name "from_date" or "from_hrs".
	 * Hence, consider this while parsing.
	 */
	/*
	 * extract the conditions from the query string(if exists). for each condition,
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
	/*
	 * extract the logical operators(AND/OR) from the query, if at all it is
	 * present. For eg: select city,winner,team1,team2,player_of_match from
	 * data/ipl.csv where season >= 2008 or toss_decision != bat and city =
	 * bangalore
	 * 
	 * the query mentioned above in the example should return a List of Strings
	 * containing [or,and]
	 */
	/*
	 * extract the aggregate functions from the query. The presence of the aggregate
	 * functions can determined if we have either "min" or "max" or "sum" or "count"
	 * or "avg" followed by opening braces"(" after "select" clause in the query
	 * string. in case it is present, then we will have to extract the same. For
	 * each aggregate functions, we need to know the following: 1. type of aggregate
	 * function(min/max/count/sum/avg) 2. field on which the aggregate function is
	 * being applied
	 * 
	 * Please note that more than one aggregate function can be present in a query
	 * 
	 * 
	 */
	public List<String> getLogicalOperators(String queryString) {
		if (null != queryString && !queryString.isEmpty()) {
			List<String> result = new ArrayList<String>();
			int startIndex = queryString.indexOf("where");
			if (startIndex > -1) {
				String whereConStr = getWhereString(queryString, startIndex);
				if (null != whereConStr && !queryString.isEmpty()) {
					String[] split = whereConStr.split(" ");
					for (String str : split) {
						if ("and".equals(str)) {
							result.add("and");
						}
						if ("or".equals(str)) {
							result.add("or");
						}
					}
				}
			}
			if (result.size() > 0)
				return result;
		}
		return null;
	}

	private String getWhereString(String queryString, int startIndex) {
		String whereConStr = "";
		int endIndex = queryString.indexOf("group");
		if (endIndex < 0) {
			endIndex = queryString.indexOf("order");
		}
		if (endIndex < 0) {
			whereConStr = queryString.substring(startIndex + "where ".length());
		}
		if (endIndex > -1)
			whereConStr = queryString.substring(startIndex + "where ".length(), endIndex - 1);
		return whereConStr;
	}

	public List<String> getFields(String queryString) {
		if (null != queryString && !queryString.isEmpty()) {
			int startIndex = queryString.indexOf("select");
			int endIndex = queryString.indexOf("from");
			String[] split = queryString.substring(startIndex + "select ".length(), endIndex - 1).split(",");
			ArrayList<String> list = new ArrayList<String>();
			for (String string : split) {
				list.add(string.trim());
			}
			return list;
		}
		return null;
	}

	public List<AggregateFunction> getAggregateFunctions(String queryString) {
		List<String> fields = getFields(queryString);
		List<AggregateFunction> list = new ArrayList<>();
		fields.forEach(field -> {
			if (field.indexOf("(") > -1) {
				String function = field.substring(0, field.indexOf("("));
				String queryField = field.substring(field.indexOf("(") + 1, field.indexOf(")"));
				list.add(new AggregateFunction(queryField, function));
			}
		});
		if (!list.isEmpty()) {
			return list;
		}
		return null;
	}

	public List<Restriction> getRestrictions(String queryString) {
		String inlower = queryString.trim();
		String tokens[] = inlower.trim().split("where");
		if (tokens.length == 1) {
			return null;
		}
		String conditions[] = tokens[1].trim().split("order by|group by");
		String indi[] = conditions[0].trim().split(" and | or ");
		List<Restriction> restrictionList = new LinkedList<Restriction>();
		for (String string : indi) {
			Optional<String> conditionOpt = Stream.of(">=", "<=", "!=", ">", "<", "=").filter(string::contains)
					.findAny();
			if (conditionOpt.isPresent()) {
				String condition = conditionOpt.get();
				String name = string.split(condition)[0].trim();
				String value = string.split(condition)[1].trim().replaceAll("'", "");
				Restriction restrictionInstance = new Restriction(name, value, condition);
				restrictionList.add(restrictionInstance);
			}
		}
		return restrictionList;
	}
}