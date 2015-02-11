package rss.categorizer.classifier;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.mllib.tree.model.Predict;

import rss.categorizer.model.StreamItem;
import rss.categorizer.util.Stopwords;
import rss.categorizer.util.StringToInt;
import scala.language;

import com.google.common.collect.Maps;

public class SequentialClassifier {

	
	public static final int window_size = 1000;
	
	public static final double max_error_threshold = 0.3;

	private static final int after_training_test_count = 5;

	private static final Double smoothing_parameter = 1.0;
	
	public static Map<String, Map<String, Integer>> wordCounts = Maps.newHashMap();
    public static Map<String, Integer> wordSums = Maps.newHashMap();
    
    public static List<StreamItem> stream;
	
    public static Stopwords sw;
    
	public static void main(String[] args) {
		
		sw = new Stopwords();
		stream = new ArrayList<StreamItem>();
		
		try {
		BufferedReader br = new BufferedReader(new FileReader("./src/main/resources/refined/part-00000"));
		String line;
		
			while ((line = br.readLine()) != null) {
				String[] terms = line.replaceAll("\\(|\\)", "").split(",");
				
				String[] terms_array = terms[1].toLowerCase().trim().replaceAll("[^0-9A-Za-z]", " ").split("\\s+", -1);;
				String doc = "";
				
				int ctr = 0;
				for(String term : terms_array) {
					if(sw.is(term)) continue;
					else {
						ctr++;
						doc += term + " ";
					}
					
					if(ctr == 10) break;
				}
				
				if(ctr < 4) continue;
				
				stream.add(new StreamItem(doc, Long.valueOf(terms[0]), terms[2]));
 
			}
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		
		
		int current_ptr = 0;
		
		current_ptr = train(current_ptr);
		double accuracy_since_last_training = -1.0f;
		int correct_count_since_last_training = 0;
		int last_training_ptr = 0;
		
		while(current_ptr < stream.size()-1) {
			if(accuracy_since_last_training > 0 && accuracy_since_last_training < max_error_threshold) {
				current_ptr = train(current_ptr);
				accuracy_since_last_training = -1.0f;
				correct_count_since_last_training = 0;
				
				if(current_ptr >= stream.size()) break;
				
				for(int ctr = 0; current_ptr < stream.size() && ctr < after_training_test_count; ctr++) {
					predict_next(++current_ptr);
				}
				
				
			}
			else {
				if(predict_next(++current_ptr)) correct_count_since_last_training++;
			}
		}
		
		System.out.println("accuracy : " + correct_count_since_last_training / (double) (current_ptr - last_training_ptr) * 100 + "%");
		
		 
		//for testing
//		for(String cur_key : wordCounts.keySet()) {
//			Integer sum = wordSums.get(cur_key);
//			
//			System.out.println(cur_key +  " aggregare sum : " + sum);
//			
//			Map<String, Integer> cond = wordCounts.get(cur_key);
//
//			for(String cur_cond_key : cond.keySet()) {
//				System.out.println(cur_key + ",  " + cur_cond_key + " : " + cond.get(cur_cond_key));
//			}
//			
//		}
		
			
	}	
	
	private static boolean predict_next(int index) {
		StreamItem data_point = stream.get(index);
		
		String[] terms_array = data_point.getTerms().split("\\s+");
		
		double maxProbability = Double.NEGATIVE_INFINITY;
		String prediction = "NA";
		
		
		int total_sum = 0;
		for(String p : wordSums.keySet() ) {
			total_sum += wordSums.get(p);
		}

		
		for(String p : wordSums.keySet() ) {
	    	   double curProbability = 0.0;
	    	   Map<String, Integer> termMap = wordCounts.get(p);
	    	   
	    	   for(int ctr = 0; ctr < terms_array.length; ctr++) {
	    		   
	    		   String term = terms_array[ctr];
	    				   
	    		   Integer temp = termMap.get(terms_array[ctr]);
	    		   if(temp == null) temp = 0;
	    		   
	    		   curProbability += Math.log((temp + smoothing_parameter) / (double) (wordSums.get(p) + smoothing_parameter));
	    	   }
	    	   
	    	   curProbability += Math.log(wordSums.get(p)/(double) total_sum);
	    	   
	    	   
	    	   if(curProbability > maxProbability) {
	    		   maxProbability = curProbability;
	    		   prediction = p;
	    	   }
	    }
		
		data_point.setPrediction(prediction);
		
		//System.out.println(prediction + " - " + data_point.getLabel());
		
		return data_point.predictedCorrect();
	}

	public static Integer train(int start_index) {
		wordCounts.clear();
		wordSums.clear();
		
		int ctr = start_index; 
		for(; ctr < window_size && ctr < stream.size(); ctr++) {
			StreamItem data_point = stream.get(ctr);
			String terms = data_point.getTerms();
			String[] terms_array = terms.split("\\s+");
			String label = data_point.getLabel();
			
			Integer count = wordSums.get(label);
			if(count == null) {
				wordSums.put(label, terms_array.length);
				
				Map<String, Integer> conditionals = Maps.newHashMap();
				
				for(String term : terms_array) {
					
					Integer count2 = conditionals.get(term);
					if(count2 == null) conditionals.put(term, 1);
					else conditionals.put(term, count2 + 1);
					
				}
				
				wordCounts.put(label, conditionals);
			}
			else {
				wordSums.put(label, count + terms_array.length);
				
				Map<String, Integer> conditionals = wordCounts.get(label);
				
				for(String term : terms_array) {
					
					Integer count2 = conditionals.get(term);
					if(count2 == null) conditionals.put(term, 1);
					else conditionals.put(term, count2 + 1);
					
				}
				
			}
		}
		return ctr;
		
	}
}
