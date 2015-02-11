package rss.categorizer.util;

public class TFIDF {

	private static final Double smoothing_param = 0.5;
	
	public static double computeTFIDF(Integer n, Integer df, Integer tf, Integer max_tf) {
		
		Double idf = Math.log(n/df);
		
		Double aug_tf = smoothing_param + ((smoothing_param * tf) / max_tf);
		
		//System.out.println(idf*aug_tf);
		
		return idf*aug_tf;
	}
	
}
