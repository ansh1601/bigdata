package uk.ac.gla.dcs.bigdata.providedfunctions;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

public class ContentFormaterMap implements MapFunction<NewsArticle,List<ContentItem>> {

	@Override
	public List<ContentItem> call(NewsArticle value) throws Exception {
		// TODO Auto-generated method stub
		return value.getContents();
	}

	

}
