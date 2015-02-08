package rss.categorizer.classifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.regression.LabeledPoint;

import rss.categorizer.config.Time;
import rss.categorizer.stream.Label;
import rss.categorizer.util.Stopwords;
import scala.Array;
import scala.Tuple3;

public class BruteforceUpdateClassifier {

private static Integer update_freq = 1000;
private static Integer window_size = 750;
	
    public static void main(String[] args) {
		
    	SparkConf conf = new SparkConf().setAppName("streamer").setMaster("local[4]");
		
		Logger.getLogger("org").setLevel(Level.FATAL);
		Logger.getLogger("akka").setLevel(Level.FATAL);
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> raw_data = sc.textFile("./src/main/resources/refined/part-00000");
		
		JavaRDD<String> raw_dictionary = sc.textFile("./src/main/resources/dictionary");
		
		HashMap<String, Integer> dictionary = new HashMap<String, Integer>();
		Stopwords sw = new Stopwords();
		
		for(String item : raw_dictionary.collect()) {
			String[] items = item.split(",");
			dictionary.put(items[0], Integer.valueOf(items[1]));
		}
		
		final Broadcast<HashMap<String, Integer>> broadcast_dict = sc.broadcast(dictionary);
		final Broadcast<Stopwords> stop_words = sc.broadcast(sw);
		
		// timestamp, text, label
		JavaRDD<Tuple3<Long, String, String>> data_tuples = raw_data.map(new Function<String, Tuple3<Long, String, String>>() {
			@Override
			public Tuple3<Long, String, String> call(String t)
					throws Exception {
						
					String[] whole = t.replaceAll("\\(|\\)", "").toLowerCase().trim().split(",");
						
					return new Tuple3<Long, String, String>(Long.valueOf(whole[0]), whole[1], whole[2]);
				}
					
		}).filter(new Function<Tuple3<Long,String,String>, Boolean>() {

					@Override
					public Boolean call(Tuple3<Long, String, String> t)
							throws Exception {
						
						//System.out.println((t._1() - 1421280000000L)/ (double) training_interval);
						if(t._1() < 1421280000000L) return false;
						else return true;
					}
					
				}).filter(new Function<Tuple3<Long,String,String>, Boolean>() {

					@Override
					public Boolean call(Tuple3<Long, String, String> t)
							throws Exception {
						
			    		String[] terms = t._2().replaceAll("[^0-9A-Za-z]", " ").split("\\s+", -1);
			    		
			    		int ctr = 0;
			    		for(String term : terms) {
			    			
			    			if(term.isEmpty()) continue;
			    			if(stop_words.value().is(term)) continue;
			    			
			    			ctr++;
			    		}
			    		
			    		if(ctr < 2) return false;
			    		else return true;
					}
			});
		
		
		Integer tuple_ptr = 0;
		List<Tuple3<Long, String, String>> data_tuples_at_driver = data_tuples.collect();
		Integer tuple_count = data_tuples_at_driver.size();
		List<Tuple3<Long, String, String>> window = new ArrayList<Tuple3<Long,String,String>>();
		Integer update_count = 0;
		NaiveBayesModel nbm;
		
		int correct_count = 0;
		int count = 0;
		
		// training - initial model building
		for(; tuple_ptr < window_size; tuple_ptr++) {
			window.add(data_tuples_at_driver.get(tuple_ptr));
		}
		JavaRDD<Tuple3<Long, String, String>> training_set = sc.parallelize(window.subList(tuple_ptr - window_size, tuple_ptr));
		System.out.println("Training");
		// setup nbm
		// timestamp, labeledpoint, label_representation
		JavaRDD<LabeledPoint> training_labeled_points = training_set.map(new Function<Tuple3<Long,String,String>, Tuple3<Long, LabeledPoint, Double>> () {

			    	@Override
			    	public Tuple3<Long, LabeledPoint, Double> call(
			    			Tuple3<Long, String, String> t) throws Exception {
			    		
			    		Double label_rep = Label.labels[Label.label_map.get(t._3())];
			    		String[] terms = t._2().replaceAll("[^0-9A-Za-z]", " ").split("\\s+", -1);
			    		
			    		HashMap<Integer, Double> map = new HashMap<Integer, Double>();
			    		
			    		HashMap<String, Integer> dict = broadcast_dict.value();
			    		
			    		for(String term : terms) {
			    			
			    			if(term.isEmpty()) continue;
			    			if(stop_words.value().is(term)) continue;
			    			
			    			Double value = map.get(dict.get(term));
			    			if(value == null) map.put(dict.get(term), 1.0);
			    			else map.put(dict.get(term), value + 1.0);
			    		
			    		}
			    		
			    		int[] indice_array = new int[map.size()];
			    		double[] value_array = new double[map.size()];
			    		
			    		int ctr = 0;
			    		for(Integer index : map.keySet()) {
			    			indice_array[ctr] = index;
			    			value_array[ctr++] = map.get(index);
			    		}
			    		
			    		Arrays.sort(indice_array);
			    		Arrays.sort(value_array);
			    		
			    		LabeledPoint lp = new LabeledPoint(label_rep, new SparseVector(dict.size(), indice_array, value_array));
			    		
			    		return new Tuple3<Long, LabeledPoint, Double>(t._1(), lp, label_rep);
			    	}
			    	}).map(new Function<Tuple3<Long,LabeledPoint,Double>, LabeledPoint>() {
					@Override
					public LabeledPoint call(Tuple3<Long, LabeledPoint, Double> t)
							throws Exception {
						return t._2();
					}
			    	});
		nbm = NaiveBayes.train(training_labeled_points.rdd());
				
		
		update_count++;
		
		while(tuple_ptr < tuple_count) {
			
			if((tuple_ptr / (double) update_freq) > update_count) {
				training_set = sc.parallelize(window.subList(tuple_ptr - window_size, tuple_ptr));
				
				System.out.println("ReTraining");
				// setup nbm
				// timestamp, labeledpoint, label_representation
				training_labeled_points = training_set.map(new Function<Tuple3<Long,String,String>, Tuple3<Long, LabeledPoint, Double>> () {

					    	@Override
					    	public Tuple3<Long, LabeledPoint, Double> call(
					    			Tuple3<Long, String, String> t) throws Exception {
					    		
					    		Double label_rep = Label.labels[Label.label_map.get(t._3())];
					    		String[] terms = t._2().replaceAll("[^0-9A-Za-z]", " ").split("\\s+", -1);
					    		
					    		HashMap<Integer, Double> map = new HashMap<Integer, Double>();
					    		
					    		HashMap<String, Integer> dict = broadcast_dict.value();
					    		
					    		for(String term : terms) {
					    			
					    			if(term.isEmpty()) continue;
					    			if(stop_words.value().is(term)) continue;
					    			
					    			Double value = map.get(dict.get(term));
					    			if(value == null) map.put(dict.get(term), 1.0);
					    			else map.put(dict.get(term), value + 1.0);
					    		
					    		}
					    		
					    		int[] indice_array = new int[map.size()];
					    		double[] value_array = new double[map.size()];
					    		
					    		int ctr = 0;
					    		for(Integer index : map.keySet()) {
					    			indice_array[ctr] = index;
					    			value_array[ctr++] = map.get(index);
					    		}
					    		
					    		Arrays.sort(indice_array);
					    		Arrays.sort(value_array);
					    		
					    		LabeledPoint lp = new LabeledPoint(label_rep, new SparseVector(dict.size(), indice_array, value_array));
					    		
					    		return new Tuple3<Long, LabeledPoint, Double>(t._1(), lp, label_rep);
					    	}
					    	}).map(new Function<Tuple3<Long,LabeledPoint,Double>, LabeledPoint>() {
							@Override
							public LabeledPoint call(Tuple3<Long, LabeledPoint, Double> t)
									throws Exception {
								return t._2();
							}
					    	});
				nbm = NaiveBayes.train(training_labeled_points.rdd());
				
				update_count++;
			}
			else {
				//System.out.println("Predicting");
				Tuple3<Long, String, String> t = data_tuples_at_driver.get(tuple_ptr++);
				window.add(t);
				
				Double label_rep = Label.labels[Label.label_map.get(t._3())];
	    		String[] terms = t._2().replaceAll("[^0-9A-Za-z]", " ").split("\\s+", -1);
	    		
	    		HashMap<Integer, Double> map = new HashMap<Integer, Double>();
	    		
	    		HashMap<String, Integer> dict = broadcast_dict.value();
	    		
	    		for(String term : terms) {
	    			
	    			if(term.isEmpty()) continue;
	    			if(stop_words.value().is(term)) continue;
	    			
	    			Double value = map.get(dict.get(term));
	    			if(value == null) map.put(dict.get(term), 1.0);
	    			else map.put(dict.get(term), value + 1.0);
	    		
	    		}
	    		
	    		int[] indice_array = new int[map.size()];
	    		double[] value_array = new double[map.size()];
	    		
	    		int ctr = 0;
	    		for(Integer index : map.keySet()) {
	    			indice_array[ctr] = index;
	    			value_array[ctr++] = map.get(index);
	    		}
	    		
	    		Arrays.sort(indice_array);
	    		Arrays.sort(value_array);
				
	    		SparseVector features = new SparseVector(dict.size(), indice_array, value_array);
	    		
				double prediction = nbm.predict(features);
				
				if(prediction == label_rep) {
					correct_count++;
				}
				count++;
			}		
			
		}
		
		
		System.out.println("accuracy: " + correct_count / (double) count * 100);

				
				
    }
}
