package com.stackroute.datamunger.query;

import java.util.HashMap;

import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.QueryParser;
import com.stackroute.datamunger.reader.CsvAggregateQueryProcessor;
import com.stackroute.datamunger.reader.CsvGroupByAggregateQueryProcessor;
import com.stackroute.datamunger.reader.CsvGroupByQueryProcessor;
import com.stackroute.datamunger.reader.CsvQueryProcessor;
import com.stackroute.datamunger.reader.QueryProcessingEngine;

@SuppressWarnings("rawtypes")
public class Query {

	/*
	 * This method will: 1.parse the query and populate the QueryParameter object
	 * 2.Based on the type of query, it will select the appropriate Query processor.
	 * In this example, we are going to work with only one Query Processor which is
	 * CsvQueryProcessor, which can work with select queries containing zero, one or
	 * multiple conditions
	 */
	public HashMap executeQuery(String queryString) {

		/* instantiate QueryParser class */
		QueryParser queryParser = new QueryParser();

		/*
		 * call parseQuery() method of the class by passing the queryString which will
		 * return object of QueryParameter
		 */
		QueryParameter queryParameter = queryParser.parseQuery(queryString);

		/*
		 * Check for Type of Query based on the QueryParameter object. In this
		 * assignment, we will process queries containing zero, one or multiple where
		 * conditions i.e. conditions, aggregate functions, order by, group by clause
		 */

		/*
		 * call the getResultSet() method of CsvQueryProcessor class by passing the
		 * QueryParameter Object to it. This method is supposed to return resultSet
		 * which is a HashMap
		 */
		
		QueryProcessingEngine csvQueryProcessor = getQueryProcessor(queryString);
		try {
			return csvQueryProcessor.getResultSet(queryParameter);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}		
	}
	
	private QueryProcessingEngine getQueryProcessor(String queryString) {
		boolean isGroupBy = queryString.contains("group");
		boolean isAggregate = queryString.contains("(");
		if (isGroupBy && isAggregate) {
			return new CsvGroupByAggregateQueryProcessor();
		} else if (isGroupBy) {
			return new CsvGroupByQueryProcessor();
		} else if (isAggregate) {
			return new CsvAggregateQueryProcessor();
		} else {
			return new CsvQueryProcessor();
		}
	}

}
