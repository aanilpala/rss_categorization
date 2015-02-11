package rss.categorizer.classifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.regression.LabeledPoint;

import rss.categorizer.config.Time;
import rss.categorizer.stream.Label;
import rss.categorizer.util.Stopwords;
import scala.Tuple3;

public class OfflineClassifier {

	private static Long training_interval = 4*24*Time.an_hour;
	
    public static void main(String[] args) {
		
    	SparkConf conf = new SparkConf().setAppName("streamer").setMaster("local[4]");
		
		Logger.getLogger("org").setLevel(Level.FATAL);
		Logger.getLogger("akka").setLevel(Level.FATAL);
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> raw_data = sc.textFile("./src/main/resources/refined/part-00000");

		System.out.println(raw_data.collect().size());
		
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
		
		
		JavaRDD<Tuple3<Long, String, String>> training_tuples = data_tuples.filter(new Function<Tuple3<Long,String,String>, Boolean>() {

			@Override
			public Boolean call(Tuple3<Long, String, String> t)
					throws Exception {
				if((t._1() - 1421280000000L) / (double) training_interval < 1.0) return true;
				else return false;
			}
			
		});
		
		JavaRDD<Tuple3<Long, String, String>> test_tuples = data_tuples.filter(new Function<Tuple3<Long,String,String>, Boolean>() {

			@Override
			public Boolean call(Tuple3<Long, String, String> t)
					throws Exception {
//------------------------------------------------------------
				if((t._1() - 1421280000000L) / (double) training_interval > 1.0) return true;
			//	if((t._1() - 1421280000000L) / (double) training_interval > 4 && (t._1() - 1421280000000L) / (double) training_interval < 5.95 ) return true;
				else return false;
			}
			
		});


//---------------------------------------
		System.out.println("First Trainingpoint: " + training_tuples.first());
		System.out.println("First Testpoint: " + test_tuples.first());


		// timestamp, labeledpoint, label_representation
		JavaRDD<Tuple3<Long, LabeledPoint, Double>> timestamped_labeled_training_points = training_tuples.map(new Function<Tuple3<Long,String,String>, Tuple3<Long, LabeledPoint, Double>> () {

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
	    });
				
				
		JavaRDD<Tuple3<Long, LabeledPoint, Double>> timestamped_labeled_test_points = test_tuples.map(new Function<Tuple3<Long,String,String>, Tuple3<Long, LabeledPoint, Double>> () {

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
	    });
		
		JavaRDD<LabeledPoint> training_labeled_points = timestamped_labeled_training_points.map(new Function<Tuple3<Long,LabeledPoint,Double>, LabeledPoint>() {

			@Override
			public LabeledPoint call(Tuple3<Long, LabeledPoint, Double> t)
					throws Exception {
				return t._2();
			}
		});
		
		
//		data_tuples.foreach(new VoidFunction<Tuple3<Long,String,String>>() {
//
//			@Override
//			public void call(Tuple3<Long, String, String> t) throws Exception {
//				System.out.println(t.toString());
//			}
//			
//		});
		
//		timestamped_labeled_test_points.foreach(new VoidFunction<Tuple3<Long,LabeledPoint,Double>>() {
//
//			@Override
//			public void call(Tuple3<Long, LabeledPoint, Double> t)
//					throws Exception {
//				System.out.println(t.toString());
//			}
//			
//		});
		
		
		NaiveBayesModel nbm = NaiveBayes.train(training_labeled_points.rdd());
		int correct_count = 0;
		int count = 0;
		ArrayList<Double> arr = new ArrayList<Double>();
		
		for(Tuple3<Long, LabeledPoint, Double> test_tuple : timestamped_labeled_test_points.collect()) {
			double prediction = nbm.predict(test_tuple._2().features());
			//System.out.println(prediction + "-" + test_tuple._3());
			if(prediction == test_tuple._3()) {
				correct_count++;
			}
			count++;
			arr.add(correct_count/(double)count *100);
		}

		List<Tuple3<Long, String, String>> listOfTrainingPoints = training_tuples.collect();
		int numberOfTrainingpoints = listOfTrainingPoints.size();
		Tuple3<Long, String, String> lastPointTraining = listOfTrainingPoints.get(numberOfTrainingpoints - 1);

		List<Tuple3<Long, String, String>> listOfTestPoints = test_tuples.collect();
		int numberOfTestpoints = listOfTestPoints.size();
		//Tuple3<Long, String, String> lastPointTest = listOfTestPoints.get(numberOfTrainingpoints - 1);
/*
		System.out.println("last Training Point: " + lastPointTraining );
	//	System.out.println("last Test Point: " + lastPointTest );
		System.out.println("Number of Testpoints: " + numberOfTestpoints + ", Number of Trainingpoints: " + numberOfTrainingpoints);
		System.out.println(count);
		System.out.println("accuracy: " + correct_count / (double) count * 100);
   */
		System.out.println("Number of Testpoints: " + numberOfTestpoints + ", Number of Trainingpoints: " + numberOfTrainingpoints);
		System.out.println(arr);
		System.out.println(arr.size());


	}		
	
}



