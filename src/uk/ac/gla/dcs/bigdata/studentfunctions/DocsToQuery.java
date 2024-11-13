package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class DocsToQuery implements MapFunction<DocumentRanking,Query> {

	/**
	 * map function to generate keys..
	 */
	private static final long serialVersionUID = -8330998338415362068L;

	public DocsToQuery() {
		// empty constructor
	}

	@Override
	public Query call(DocumentRanking value) throws Exception {
		
		return value.getQuery();
	}

}
