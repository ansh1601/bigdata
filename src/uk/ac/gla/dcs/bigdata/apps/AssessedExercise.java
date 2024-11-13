package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.*;
import uk.ac.gla.dcs.bigdata.studentfunctions.*;
/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be over ridden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	
	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exercise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		//newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json";
		// Call the student's code
		
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		
		
	}
	
	
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news article.
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile).repartition(10); // read in files as string rows, one row per article
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle
		
		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------
		
		
		//Extracting from news Article necessary stuffs into a new structure..
		
		/*
		 * Creating accumulators so that while performing tokenization on this new Structure it also
		 * returns length of document and also the word count..
		 */
		LongAccumulator wordCountAccumulator = spark.sparkContext().longAccumulator();
		CollectionAccumulator<String> corpusTermsAccumulator = spark.sparkContext().collectionAccumulator();
		
		//Mapping FilteredContentNewsArticle into a new structure to have its tokens along side.
		Dataset<FilteredArticle> filteredArticle = news.flatMap(
				new ArticleProcessorMap(
						wordCountAccumulator,corpusTermsAccumulator), 
				Encoders.bean(FilteredArticle.class));
		/*
		 * Calling a spark action to be able to get value of accumulators..
		 * This is necessary... 
		 * DO NOT DELETE
		 */
		long docCount = filteredArticle.count();
		
		
		long wordCount = wordCountAccumulator.value();
		double averageTerms = wordCount/docCount;
		
		List<String> corpusTerms = corpusTermsAccumulator.value();
		
		/*
		 * Creating a broadcast for this corpusTerms Accumulator to pass as a parameter in a constructor
		 * for new Flatmap function that return Tuple2 of Query Terms with its overall term Counts in the 
		 * entire length of corpus..
		 */
		
		Broadcast<List<String>> broadcastTerms = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(corpusTerms);
		
		
			
		Dataset<Tuple2<String,Integer>> queryTermCounts = queries.flatMap(
				new QueryTermCount(broadcastTerms),Encoders.tuple(Encoders.STRING(), Encoders.INT()));
		
		List<Tuple2<String,Integer>> listqueryTermCounts = queryTermCounts.collectAsList();
		
		/*
		 * Again creating another broadcast for this tuple alongside a flat map for original query because it needs to be passed into another
		 * flatMap function that returns a document Score for each query ...
		 * Also creating broadcast for Query in-order to be able to recognize query terms belonging to query
		 */
		
		Broadcast<List<Tuple2<String,Integer>>> broadcastTuple = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(listqueryTermCounts);
		
		List<Query> queryDataList = queries.collectAsList();
		Broadcast<List<Query>> broadcastQuery =  JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryDataList);
		
		/*
		 * The very main flat map function that takes a lots of broadcast variables and returns
		 * a dataset of type document ranking for the dataset given as news article ..  
		 */
		Dataset<DocumentRanking> docData = filteredArticle.flatMap(
				new DocsByDPHScore(docCount,averageTerms,
						broadcastTuple,broadcastQuery),
				Encoders.bean(DocumentRanking.class));

		/*
		 * Creating a key valued grouped Dataset for this obtained dataset of document ranking which
		 * is grouped by key (Query) so that list can be merged for that query ...
		 */
		
		KeyValueGroupedDataset<Query,DocumentRanking> docByQuery = docData.groupByKey(
											new DocsToQuery(), Encoders.bean(Query.class));
		
		
		
		//Applying FlatMapGroup function on this new key Value group Dataset..
		
		Dataset<DocumentRanking> finalDocs = docByQuery.flatMapGroups(
				new FinalDocsGroupedByQuery(),Encoders.bean(DocumentRanking.class)) ;
		
		List<DocumentRanking> listDocs = finalDocs.collectAsList();
		
		
		return listDocs; // replace this with the the list of DocumentRanking output by your topology
	}
	
	
}
