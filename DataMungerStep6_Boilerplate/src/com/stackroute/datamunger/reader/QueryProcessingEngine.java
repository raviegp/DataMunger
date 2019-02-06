package com.stackroute.datamunger.reader;
import java.util.HashMap;
import com.stackroute.datamunger.query.parser.QueryParameter;

@SuppressWarnings("rawtypes")
public interface QueryProcessingEngine {
	public HashMap getResultSet(QueryParameter queryParameter) throws Exception;
	
}