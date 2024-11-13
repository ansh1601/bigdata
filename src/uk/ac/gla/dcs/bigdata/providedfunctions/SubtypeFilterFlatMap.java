package uk.ac.gla.dcs.bigdata.providedfunctions;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class SubtypeFilterFlatMap implements FlatMapFunction<NewsArticle,String> {

	private static final long serialVersionUID = 1L;

	@Override
	public Iterator<String> call(NewsArticle N) throws Exception {
		
		// Using a String Builder to extract particular elements from the object Content Item.
		// 
		
		StringBuilder keyBuilder = new StringBuilder();
		List<ContentItem> contents = new ArrayList<ContentItem>();
		List<String> relevantContents = new ArrayList<String>();
		contents.addAll(N.getContents());
		for (ContentItem C:contents) {
 		keyBuilder.append(C.getContent());
		keyBuilder.append(C.getBio());
		keyBuilder.append(C.getType());
		
		relevantContents.add(keyBuilder.toString());
		}
		
		return relevantContents.iterator();
}
}
	


