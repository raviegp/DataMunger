package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IntSummaryStatistics;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.GroupedDataSet;
import com.stackroute.datamunger.query.Header;
import com.stackroute.datamunger.query.Row;
import com.stackroute.datamunger.query.RowDataTypeDefinitions;
import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.QueryParameter;

/* This is the CsvGroupByAggregateQueryProcessor class used for evaluating queries with 
 * aggregate functions and group by clause*/
@SuppressWarnings("rawtypes")
public class CsvGroupByAggregateQueryProcessor implements QueryProcessingEngine {
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
		queryParameter.getFileName();
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
				for (int i = 0; i < queryParameter.getFields().size(); i++) {
					if (queryParameter.getFields().get(i).equals("*")) {
						for (int j = 0; j < rowFields.length; j++) {
							rowMap.put(headers[j].trim(), rowFields[j]);
						}
					} else {
						String field = queryParameter.getFields().get(i);
						if (field.indexOf("(") > -1) {
							field = field.substring(field.indexOf("(") + 1, field.indexOf(")"));
						}
						if (field.equals("*")) {
							rowMap.put(field, "");
						} else {
							rowMap.put(field, rowFields[headerMap.get(field)]);
						}
					}
				}
				dataSet.put(setRowIndex++, rowMap);
			}
		}
		br.close();
		/*
		 * if the overall condition expression evaluates to true, then we need to check
		 * for the existence for group by clause in the Query Parameter. The dataSet
		 * generated after processing a group by with aggregate clause is completely
		 * different from a dataSet structure(which contains multiple rows of data). In
		 * case of queries containing group by clause and aggregate functions, the
		 * resultSet will contain multiple dataSets, each of which will be assigned to
		 * the group by column value i.e. for all unique values of the group by column,
		 * aggregates will have to be calculated. Hence, we will use
		 * GroupedDataSet<String,Object> to store the same and not DataSet<Long,Row>.
		 * Please note we will process queries containing one group by column only for
		 * this example.
		 */
		// return groupedDataSet object
		return getgroupedData(dataSet, queryParameter.getAggregateFunctions(), headerMap, rowDataTypeDefinitionMap,
				queryParameter.getGroupByFields());
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

	public GroupedDataSet getgroupedData(DataSet dataset, List<AggregateFunction> aggregateFunction, Header header,
			RowDataTypeDefinitions rowDataType, List<String> groupByFields) {
		GroupedDataSet groupedDataSet = new GroupedDataSet();
		for (int k = 0; k < groupByFields.size(); k++) {
			HashMap<String, List<Row>> groupedDataMap = new LinkedHashMap<String, List<Row>>();
			String groupByField = groupByFields.get(k);
			int distictRows = getDistinctValuesOfColumnFromDataSet(dataset, groupByField).size();
			List<List<Row>> rowLists = new ArrayList<List<Row>>();
			List<String> distinctValues = getDistinctValuesOfColumnFromDataSet(dataset, groupByField);
			for (int i = 0; i < distictRows; i++) {
				rowLists.add(i, new ArrayList<Row>());
			}
			for (int i = 0; i < dataset.size(); i++) {
				int ListIndex = distinctValues.indexOf(dataset.get((long) (i + 1)).get(groupByField));
				rowLists.get(ListIndex).add(dataset.get((long) (i + 1)));
			}
			for (int i = 0; i < rowLists.size(); i++) {
				groupedDataMap.put(distinctValues.get(i), rowLists.get(i));
			}
			setValuesToGroupedDataSet(groupedDataSet, groupedDataMap, aggregateFunction, header, rowDataType);
		}
		return groupedDataSet;
	}

	public List<String> getDistinctValuesOfColumnFromDataSet(DataSet dataset, String field) {
		HashSet<String> hashset = new HashSet<String>();
		List<String> distinctValues = new ArrayList<>();
		for (int i = 0; i < dataset.size(); i++) {
			hashset.add(dataset.get((long) (i + 1)).get(field));
		}
		distinctValues.addAll(hashset);
		return distinctValues;
	}

	public GroupedDataSet setValuesToGroupedDataSet(GroupedDataSet groupeddataset,
			HashMap<String, List<Row>> groupedDataMap, List<AggregateFunction> aggregateFunction, Header header,
			RowDataTypeDefinitions rowDataType) {
		for (int j = 0; j < aggregateFunction.size(); j++) {
			String field = aggregateFunction.get(j).getField();
			String function = aggregateFunction.get(j).getFunction();
			List<String> distinctGroupedValues = new ArrayList<>();
			distinctGroupedValues.addAll(groupedDataMap.keySet());
			Set<String> headers = header.keySet();
			List<String> headersValues = new ArrayList<>();
			headersValues.addAll(headers);
			Integer num = 0;
			Double d = 0.00;
			if (field.equals("*")) {
				for (int k = 0; k < groupedDataMap.size(); k++) {
					for (int l = 0; l < header.size(); l++) {
						if (function.equals("count")) {
							groupeddataset.put(distinctGroupedValues.get(k),
									groupedDataMap.get(distinctGroupedValues.get(k)).size());
						} else if ((rowDataType.get(header.get(headersValues.get(l))))
								.equals(num.getClass().getName())) {
							List<Integer> values = new ArrayList<>();
							for (int i = 0; i < groupedDataMap.get(distinctGroupedValues.get(k)).size(); i++) {
								values.add(Integer.valueOf(groupedDataMap.get(distinctGroupedValues.get(k)).get(i)
										.get(headersValues.get(l))));
							}
							IntSummaryStatistics summaryStatistics = values.stream().mapToInt((x) -> x)
									.summaryStatistics();
							if (function.equals("min")) {
								groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getMin());
							} else if (function.equals("max")) {
								groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getMax());
							} else if (function.equals("sum")) {
								groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getSum());
							} else if (function.equals("avg")) {
								groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getAverage());
							} else if (function.equals("count")) {
								groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getCount());
							}
						} else if ((rowDataType.get(header.get(headersValues.get(l)))).equals(d.getClass().getName())) {
							List<Double> values = new ArrayList<>();
							for (int i = 0; i < groupedDataMap.get(distinctGroupedValues.get(k)).size(); i++) {
								values.add(Double.valueOf(groupedDataMap.get(distinctGroupedValues.get(k)).get(i)
										.get(headersValues.get(l))));
							}
							DoubleSummaryStatistics summaryStatistics = values.stream().mapToDouble(m -> m)
									.summaryStatistics();
							if (function.equals("min")) {
								groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getMin());
							} else if (function.equals("max")) {
								groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getMax());
							} else if (function.equals("sum")) {
								groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getSum());
							} else if (function.equals("avg")) {
								groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getAverage());
							} else if (function.equals("count")) {
								groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getCount());
							}
						}
					}
				}
			} else {
				for (int k = 0; k < groupedDataMap.size(); k++) {
					if ((rowDataType.get(header.get(field))).equals(num.getClass().getName())) {
						List<Integer> values = new ArrayList<>();
						for (int i = 0; i < groupedDataMap.get(distinctGroupedValues.get(k)).size(); i++) {
							values.add(Integer
									.valueOf(groupedDataMap.get(distinctGroupedValues.get(k)).get(i).get(field)));
						}
						IntSummaryStatistics summaryStatistics = values.stream().mapToInt((x) -> x).summaryStatistics();
						if (function.equals("min")) {
							groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getMin());
						} else if (function.equals("max")) {
							groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getMax());
						} else if (function.equals("sum")) {
							groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getSum());
						} else if (function.equals("avg")) {
							groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getAverage());
						} else if (function.equals("count")) {
							groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getCount());
						}
					} else if ((rowDataType.get(header.get(field))).equals(d.getClass().getName())) {
						List<Double> values = new ArrayList<>();
						for (int i = 0; i < groupedDataMap.get(distinctGroupedValues.get(k)).size(); i++) {
							values.add(
									Double.valueOf(groupedDataMap.get(distinctGroupedValues.get(k)).get(i).get(field)));
						}
						DoubleSummaryStatistics summaryStatistics = values.stream().mapToDouble(m -> m)
								.summaryStatistics();
						if (function.equals("min")) {
							groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getMin());
						} else if (function.equals("max")) {
							groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getMax());
						} else if (function.equals("sum")) {
							groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getSum());
						} else if (function.equals("avg")) {
							groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getAverage());
						} else if (function.equals("count")) {
							groupeddataset.put(distinctGroupedValues.get(k), summaryStatistics.getCount());
						}
					} else {
						if (function.equals("count")) {
							groupeddataset.put(distinctGroupedValues.get(k),
									groupedDataMap.get(distinctGroupedValues.get(k)).size());
						}
					}
				}
			}
		}
		return groupeddataset;
	}
}