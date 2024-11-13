package uk.ac.gla.dcs.bigdata.studentfunctions;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.FilteredArticle;

public class ArticleProcessorMap implements FlatMapFunction<NewsArticle,FilteredArticle>{

	/**
	 * process contents of news article based on given condition
	 * and filter out irrelevant news article
	 * use of provided utility text preprocessor
	 */
	private static final long serialVersionUID = 1L;
	private transient TextPreProcessor processor;
	LongAccumulator wordCountAccumulator;
	CollectionAccumulator<String> corpusTermsAccumulator;


	public ArticleProcessorMap(LongAccumulator wordCountAccumulator,
			CollectionAccumulator<String> corpusTermsAccumulator) {
		super();
		this.wordCountAccumulator = wordCountAccumulator;
		this.corpusTermsAccumulator = corpusTermsAccumulator;
	}


	@Override
	public Iterator<FilteredArticle> call(NewsArticle N) throws Exception {
		
		// Applying given Text Preprocessor on the documents .
		
		List<ContentItem> contents = new ArrayList<ContentItem>();
		StringBuilder key = new StringBuilder();
		key.append("");
		int j=0;
		
		contents.addAll(N.getContents());
		
		for(ContentItem C:contents) {
						
						if(N.getTitle()!=null&&C!=null&&j<5&&C.getContent()!=null)
				{
						key.append(N.getTitle());
						key.append(C.getContent());
						if(C.getFullcaption()!=null)key.append(C.getFullcaption());
						if(C.getBio()!=null) key.append(C.getBio());
						}
						else
							break;
						
					j++;
		}
		
		String cont = key.toString();
		
		if (processor==null) processor = new TextPreProcessor();
		/*String contents = value.getContent();
		NewsArticle article = value.getArticle();*/
			if(cont!=""&&N.getTitle()!=null) {
						
				List<String> contentTerms = processor.process(cont);
				int[] contentTermCounts = new int[contentTerms.size()];
				for (int i =0; i<contentTerms.size(); i++) {
					contentTermCounts[i] = (int)1;
				}
				
				FilteredArticle doc = new FilteredArticle(cont,contentTerms,
						contentTermCounts,N);
				for(String S:contentTerms) {
					corpusTermsAccumulator.add(S);
					wordCountAccumulator.add(1);
				}
				List<FilteredArticle> tokenDocs = new ArrayList<FilteredArticle>(1);
				tokenDocs.add(doc);
				return tokenDocs.iterator();
					}
					else {
						
						List<FilteredArticle> emptyList = new ArrayList<FilteredArticle>(0);
					return emptyList.iterator();
					
				}
		
	}
	
}
