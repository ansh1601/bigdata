package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;

public class FinalDocsGroupedByQuery implements FlatMapGroupsFunction<Query,DocumentRanking,DocumentRanking> {

	/**
	 * grouping of DocumentRanking based on key query so that 
	 * it can be processed in parallel for each query
	 */
	private static final long serialVersionUID = -793421948810671318L;

	public FinalDocsGroupedByQuery() {
		// empty Constructor
	}

	@Override
	public Iterator<DocumentRanking> call(Query key, Iterator<DocumentRanking> values) throws Exception {
		
		List<RankedResult> lr = new ArrayList<RankedResult>();
		List<DocumentRanking> dr = new ArrayList<DocumentRanking>();
		List<RankedResult> finalList = new ArrayList<RankedResult>();
		List<RankedResult> dumpList = new ArrayList<RankedResult>();
		while(values.hasNext()) {
			DocumentRanking doc = values.next();
			lr.addAll(doc.getResults());
		}
		Collections.sort(lr);
		Collections.reverse(lr);
		finalList.add(lr.get(0));
		for(int i=1;i<lr.size();i++) {
			if(finalList.size()<10) {
				if(TextDistanceCalculator.similarity(
						lr.get(i).getArticle().getTitle(),lr.get(i-1).getArticle().getTitle())>0.5) {
					finalList.add(lr.get(i));
				}
				else {
					dumpList.add(lr.get(i));
				}
			}
		}
		Collections.sort(dumpList);
		Collections.reverse(dumpList);
			int dif =10- finalList.size();
			for(int j=0;j<dif;j++) {
				finalList.add(dumpList.get(j));
			}
		
		dr.add(new DocumentRanking(key,finalList));
		return dr.iterator();
		//return new DocumentRanking(key,finalList);
	}

}
