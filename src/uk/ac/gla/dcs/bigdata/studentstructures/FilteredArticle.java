package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class FilteredArticle implements Serializable {

	
		
		/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
		/* Structure which contains different attributes to identify documents and also 
		 * convert titles and contents into tokens as well as term counts fields to find their relevant
		 * DPH Score for performing further tasks in the assessed exercise.
		 */
	
	public FilteredArticle() {
		//empty Constructor in Structure...
	}
		String contents;
		List<String> contentsTerms;
		int[] contentsTermCounts;
		NewsArticle article;
		public FilteredArticle(String contents, List<String> contentsTerms,
				int[] contentsTermCounts,NewsArticle article) {
			super();
			this.contents = contents;
			this.contentsTerms = contentsTerms;
			this.contentsTermCounts = contentsTermCounts;
			this.article = article;
		}
		public NewsArticle getArticle() {
			return article;
		}
		public void setArticle(NewsArticle article) {
			this.article = article;
		}
		
		public String getContents() {
			return contents;
		}
		public void setContents(String contents) {
			this.contents = contents;
		}
		public List<String> getContentsTerms() {
			return contentsTerms;
		}
		public void setContentsTerms(List<String> contentsTerms) {
			this.contentsTerms = contentsTerms;
		}
		public int[] getContentsTermCounts() {
			return contentsTermCounts;
		}
		public void setContentsTermCounts(int[] contentsTermCounts) {
			this.contentsTermCounts = contentsTermCounts;
		}
		
		

}
