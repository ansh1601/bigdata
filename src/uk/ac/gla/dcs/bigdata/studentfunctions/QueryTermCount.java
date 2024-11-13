package uk.ac.gla.dcs.bigdata.studentfunctions;


import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class QueryTermCount 
implements FlatMapFunction<Query,Tuple2<String,Integer>>{

	/**
	 *  Using flatmap to map each query term to its count in the entire dataset.
	 */
	private static final long serialVersionUID = -5345413654804980644L;
	private Broadcast<List<String>> broadcastTerms;
	

	public QueryTermCount(Broadcast<List<String>> broadcastTerms){
		this.broadcastTerms = broadcastTerms;
		}

	@Override
	public java.util.Iterator<Tuple2<String, Integer>> call(Query Q) throws Exception {
		
		List<Tuple2<String,Integer>> queryFreq = new ArrayList<Tuple2<String,Integer>>();
		List<String> corpus = broadcastTerms.value();
		List<String> terms = Q.getQueryTerms();
		for(String term:terms) {
			int globalTermFreq =0;
			for(String S:corpus) {
				if(S.equals(term)) {
					globalTermFreq++;
				}
			}
			Tuple2<String,Integer> tup = new Tuple2<String,Integer>(term,globalTermFreq);
			queryFreq.add(tup);
		}
		
return queryFreq.iterator();
	}

	


}
