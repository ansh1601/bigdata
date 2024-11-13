package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.FilteredArticle;

public class DocsByDPHScore implements FlatMapFunction<FilteredArticle,DocumentRanking>{

	/**
	 * find score of every news article for a query using parameters passed to a 
	 * DPH Scorer function..
	 * 
	 */
	private static final long serialVersionUID = 1L;
	long docCount;
	double averageTerms;
	Broadcast<List<Tuple2<String,Integer>>> broadcastTuple;
	Broadcast<List<Query>> broadcastQuery;
	public DocsByDPHScore(long docCount, double averageTerms,
			Broadcast<List<Tuple2<String, Integer>>> broadcastTuple, Broadcast<List<Query>> broadcastQuery) {
		super();
		this.docCount = docCount;
		this.averageTerms = averageTerms;
		this.broadcastTuple = broadcastTuple;
		this.broadcastQuery = broadcastQuery;
	}

	@Override
	public Iterator<DocumentRanking> call(FilteredArticle D) throws Exception {
		List<Tuple2<String,Integer>> listTuple = broadcastTuple.value();
		List<Query> queryList = broadcastQuery.value();
		List<String> contents = D.getContentsTerms();
		NewsArticle article = D.getArticle();
		//Iterator<Tuple2<String,Integer>> iterTuple1 = listTuple.iterator();
		List<DocumentRanking> docRank = new ArrayList<DocumentRanking>();
		int currentSize = contents.size();
		for(Query Q:queryList) {
			double score = 0;
			List<String> qterms = Q.getQueryTerms();
			for(String S:qterms) {
				double termScore=0;
					Iterator<Tuple2<String,Integer>> iterTuple = listTuple.iterator();
					while(iterTuple.hasNext()) {
						Tuple2<String,Integer> T = iterTuple.next();
						short termFreq = 0;
					if(T._1().equals(S)){
						
						for(int i =0;i<currentSize;i++) {
							if(contents.get(i).equals(S)) {
								termFreq++;
							}
						}
						termScore = DPHScorer.getDPHScore(termFreq, T._2(), currentSize, averageTerms,docCount);
					}
					
				}
					if(Double.isNaN(termScore)) {
						termScore=0;
					}
					score+=termScore;
			}
			
			score = score/qterms.size();
			if(!(Double.isNaN(score))&&score>0) {
				List<RankedResult> ranked = new ArrayList<RankedResult>();
				ranked.add(new RankedResult(article.getId(),article,score));
				
				docRank.add(new DocumentRanking(Q,ranked));
			}
		}
		
		return docRank.iterator();
	}

}
