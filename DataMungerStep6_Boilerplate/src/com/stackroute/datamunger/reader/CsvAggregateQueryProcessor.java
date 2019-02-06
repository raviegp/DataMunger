package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.List;
import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.Header;
import com.stackroute.datamunger.query.Row;
import com.stackroute.datamunger.query.RowDataTypeDefinitions;
import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.QueryParameter;

/* This is the CsvAggregateQueryProcessor class used for evaluating queries with 
 * aggregate functions without group by clause*/
@SuppressWarnings("rawtypes")
public class CsvAggregateQueryProcessor implements QueryProcessingEngine {
	/*
	 * This method will take QueryParameter object as a parameter which contains the
	 * parsed query and will process and populate the ResultSet
	 */
	public HashMap getResultSet(QueryParameter queryParameter) throws Exception {
		/*
		 * initialize BufferedReader to read from the file which is mentioned in
		 * QueryParameter. Consider Handling Exception related to file reading.
		 */
		FileReader fileReader = new FileReader(queryParameter.getFileName());
		BufferedReader br = new BufferedReader(fileReader);
		/*
		 * read the first line which contains the header. Please note that the headers
		 * can contain spaces in between them. For eg: city, winner
		 */
		String[] headers = br.readLine().split(",");
		br.mark(1);
		/*
		 * read the next line which contains the first row of data. We are reading this
		 * line so that we can determine the data types of all the fields. Please note
		 * that ipl.csv file contains null value in the last column. If you do not
		 * consider this while splitting, this might cause exceptions later
		 */
		String[] fields = br.readLine().split(",", headers.length);
		/*
		 * populate the header Map object from the header array. header map is having
		 * data type <String,Integer> to contain the header and it's index.
		 */
		Header headerMap = new Header();
		for (int i = 0; i < headers.length; i++) {
			headerMap.put(headers[i].trim(), i);
		}
		/*
		 * We have read the first line of text already and kept it in an array. Now, we
		 * can populate the dataTypeDefinition Map object. dataTypeDefinition map is
		 * having data type <Integer,String> to contain the index of the field and it's
		 * data type. To find the dataType by the field value, we will use getDataType()
		 * method of DataTypeDefinitions class
		 */
		RowDataTypeDefinitions rowDataTypeDefinitionMap = new RowDataTypeDefinitions();
		for (int i = 0; i < fields.length; i++) {
			rowDataTypeDefinitionMap.put(i, (String) DataTypeDefinitions.getDataType(fields[i]));
		}
		/*
		 * once we have the header and dataTypeDefinitions maps populated, we can start
		 * reading from the first line. We will read one line at a time, then check
		 * whether the field values satisfy the conditions mentioned in the query,if
		 * yes, then we will add it to the resultSet. Otherwise, we will continue to
		 * read the next line. We will continue this till we have read till the last
		 * line of the CSV file.
		 */
		/* reset the buffered reader so that it can start reading from the first line */
		br.reset();
		/*
		 * skip the first line as it is already read earlier which contained the header
		 */
		/* read one line at a time from the CSV file till we have any lines left */
		/*
		 * once we have read one line, we will split it into a String Array. This array
		 * will continue all the fields of the row. Please note that fields might
		 * contain spaces in between. Also, few fields might be empty.
		 */
		/*
		 * if there are where condition(s) in the query, test the row fields against
		 * those conditions to check whether the selected row satifies the conditions
		 */
		/*
		 * from QueryParameter object, read one condition at a time and evaluate the
		 * same. For evaluating the conditions, we will use evaluateExpressions() method
		 * of Filter class. Please note that evaluation of expression will be done
		 * differently based on the data type of the field. In case the query is having
		 * multiple conditions, you need to evaluate the overall expression i.e. if we
		 * have OR operator between two conditions, then the row will be selected if any
		 * of the condition is satisfied. However, in case of AND operator, the row will
		 * be selected only if both of them are satisfied.
		 */
		/*
		 * check for multiple conditions in where clause for eg: where salary>20000 and
		 * city=Bangalore for eg: where salary>20000 or city=Bangalore and dept!=Sales
		 */
		List<String> fieldsMap = new ArrayList<String>();
		List<String> fields2 = queryParameter.getFields();
		for (String string2 : fields2) {
			if (string2.indexOf("(") > -1) {
				fieldsMap.add(string2.substring(string2.indexOf("(") + 1, string2.indexOf(")")));
			}
		}
		DataSet dataSet = new DataSet();
		long setRowIndex = 1;
		Filter filter = new Filter();
		String line;
		while ((line = br.readLine()) != null) {
			String[] rowFields = line.split(",", headers.length);
			boolean result = false;
			ArrayList<Boolean> bools = new ArrayList<Boolean>();
			if (queryParameter.getRestrictions() == null)
				result = true;
			else {
				for (int i = 0; i < queryParameter.getRestrictions().size(); i++) {
					int index = headerMap.get(queryParameter.getRestrictions().get(i).getPropertyName());
					bools.add(filter.evaluateExpression(queryParameter.getRestrictions().get(i),
							rowFields[index].trim(), rowDataTypeDefinitionMap.get(index)));
				}
				result = solveOperators(bools, queryParameter.getLogicalOperators());
			}
			if (result) {
				Row rowMap = new Row();
				for (int i = 0; i < fieldsMap.size(); i++) {
					if (queryParameter.getFields().get(i).equals("*")) {
						for (int j = 0; j < rowFields.length; j++) {
							rowMap.put(headers[j].trim(), rowFields[j]);
						}
					} else {
						rowMap.put(fieldsMap.get(i), rowFields[headerMap.get(fieldsMap.get(i))]);
					}
				}
				dataSet.put(setRowIndex++, rowMap);
			}
		}
		br.close();
		/*
		 * if the overall condition expression evaluates to true, then we need to check
		 * for the existence for aggregate functions in the Query Parameter. Please note
		 * that there can be more than one aggregate functions existing in a query. The
		 * dataSet generated after processing any aggregate function is completely
		 * different from a dataSet structure(which contains multiple rows of data). In
		 * case of queries containing aggregate functions, each row of the resultSet
		 * will contain the key(for e.g. 'count(city)') and it's aggregate value. Hence,
		 * we will use GroupedDataSet<String,Object> to store the same and not
		 * DataSet<Long,Row>. we will process all the five aggregate functions i.e. min,
		 * max, avg, sum, count.
		 */
		// return groupedDataSet object
		return getAgregateRowValues(dataSet, queryParameter.getAggregateFunctions(), headerMap,
				rowDataTypeDefinitionMap);
	}

	private boolean solveOperators(ArrayList<Boolean> bools, List<String> operators) {
		if (bools.size() == 1) {
			return bools.get(0);
		} else if (bools.size() == 2) {
			if (operators.get(0).matches("AND|and"))
				return bools.get(0) & bools.get(1);
			else
				return bools.get(0) | bools.get(1);
		} else if (bools.size() == 3) {
			int i = operators.indexOf("AND|and");
			if (i < 0)
				return bools.get(0) | bools.get(1) | bools.get(2);
			else if (i == 0)
				return bools.get(0) & bools.get(1) | bools.get(2);
			else if (i == 1)
				return bools.get(0) | bools.get(1) & bools.get(2);
			else
				return false;
		} else
			return false;
	}

	public HashMap getAgregateRowValues(DataSet dataset, List<AggregateFunction> aggregateFunction, Header header,
			RowDataTypeDefinitions rowDataType) {
		HashMap<String, Object> map = new HashMap<String, Object>();
		for (int j = 0; j < aggregateFunction.size(); j++) {
			String field = aggregateFunction.get(j).getField();
			String function = aggregateFunction.get(j).getFunction();
			Integer num = 0;
			Double d = 0.00;
			if ((rowDataType.get(header.get(field))).equals(num.getClass().getName())) {
				DecimalFormat formatter = new DecimalFormat("0.000");
				List<Integer> values = new ArrayList<>();
				for (int i = 1; i <= dataset.size(); i++) {
					values.add(Integer.valueOf((dataset.get((long) i)).get(field)));
				}
				IntSummaryStatistics summaryStatistics = values.stream().mapToInt(m -> m).summaryStatistics();
				if (function.equals("min")) {
					map.put(function + "(" + field + ")", summaryStatistics.getMin());
				} else if (function.equals("max")) {
					map.put(function + "(" + field + ")", summaryStatistics.getMax());
				} else if (function.equals("sum")) {
					Double val = Double.valueOf(summaryStatistics.getSum());
					formatter.format(val);
					map.put(function + "(" + field + ")", val);
				} else if (function.equals("avg")) {
					Double val = Double.valueOf(summaryStatistics.getAverage());
					formatter.format(val);
					map.put(function + "(" + field + ")", val);
				} else if (function.equals("count")) {
					map.put(function + "(" + field + ")", summaryStatistics.getCount());
				}
			} else if ((rowDataType.get(header.get(field))).equals(d.getClass().getName())) {
				DecimalFormat formatter = new DecimalFormat("0.000");
				List<Double> values = new ArrayList<>();
				for (int i = 1; i <= dataset.size(); i++) {
					values.add(Double.valueOf((dataset.get((long) i)).get(field)));
				}
				DoubleSummaryStatistics summaryStatistics = values.stream().mapToDouble(m -> m).summaryStatistics();
				if (function.equals("min")) {
					map.put(function + "(" + field + ")", summaryStatistics.getMin());
				} else if (function.equals("max")) {
					map.put(function + "(" + field + ")", summaryStatistics.getMax());
				} else if (function.equals("sum")) {
					Double val = Double.valueOf(summaryStatistics.getSum());
					formatter.format(val);
					map.put(function + "(" + field + ")", val);
				} else if (function.equals("avg")) {
					Double val = Double.valueOf(summaryStatistics.getAverage());
					formatter.format(val);
					map.put(function + "(" + field + ")", val);
					map.put(function + "(" + field + ")", val);
				} else if (function.equals("count")) {
					map.put(function + "(" + field + ")", summaryStatistics.getCount());
				}
			} else {
				List<String> values = new ArrayList<>();
				for (int i = 1; i <= dataset.size(); i++) {
					values.add((dataset.get((long) i)).get(field));
				}
				List<Integer> nullValueIndex = new ArrayList<>();
				for (int k = 0; k < values.size(); k++) {
					if (values.get(k).equals("")) {
						nullValueIndex.add(k);
					}
				}
				for (int nulVal : nullValueIndex) {
					values.remove(nulVal);
				}
				if (function.equals("count")) {
					map.put(function + "(" + field + ")", values.size());
				}
			}
		}
		return map;
	}
}