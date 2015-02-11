package rss.categorizer.classifier;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
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
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.regression.LabeledPoint;

import rss.categorizer.stream.Label;
import rss.categorizer.util.Stopwords;
import scala.Tuple2;
import scala.Tuple3;

public class IncrementalUpdateClassifier {

	private static Integer incremental_update_freq = 50;
	private static Integer initial_training = 500;
	private static Double learning_rate_index = Math.tan(Math.PI/32);
		
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
			
			List<Tuple3<Long, String, String>> data_tuples_at_the_driver = data_tuples.collect();
			List<Tuple3<Long, Integer, Boolean>> predictions = new ArrayList<Tuple3<Long, Integer, Boolean>>();
			
			Integer tuple_count = data_tuples_at_the_driver.size();
			
			Integer tuple_ptr = initial_training;
			
			JavaRDD<Tuple3<Double, String, String>> old_tuples;
			JavaRDD<Tuple3<Double, String, String>> training_tuples = sc.parallelize(data_tuples_at_the_driver.subList(0,  initial_training)).map(new Function<Tuple3<Long,String,String>, Tuple3<Double,String,String>>() {

				@Override
				public Tuple3<Double, String, String> call(
						Tuple3<Long, String, String> v1) throws Exception {
					return new Tuple3<Double, String, String>(1.0, v1._2(), v1._3());
				}
			});
			
			JavaRDD<LabeledPoint> training_lps = training_tuples.map(new Function<Tuple3<Double,String,String>, LabeledPoint>() {
				
				@Override
		    	public LabeledPoint call(
		    			Tuple3<Double, String, String> t) throws Exception {
		    		
		    		Double label_rep = Label.labels[Label.label_map.get(t._3())];
		    		String[] terms = t._2().replaceAll("[^0-9A-Za-z]", " ").split("\\s+", -1);
		    		
		    		HashMap<Integer, Double> map = new HashMap<Integer, Double>();
		    		
		    		HashMap<String, Integer> dict = broadcast_dict.value();
		    		
		    		for(String term : terms) {
		    			
		    			if(term.isEmpty()) continue;
		    			if(stop_words.value().is(term)) continue;
		    			
		    			Double value = map.get(dict.get(term));
		    			if(value == null) map.put(dict.get(term), t._1());
		    			else map.put(dict.get(term), value +  t._1());
		    		
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
		    		
		    		return lp;
		    	}
			});
			
			NaiveBayesModel nbm;
			nbm = NaiveBayes.train(training_lps.rdd());
			
			
			boolean just_trained = true;
			
			int local_correct_count = 0;
			int local_count = 0;
			double last_accuracy = Double.NaN;
			
			List<Double> local_learning_rate = new ArrayList<Double>();
			
			local_learning_rate.add(Math.abs(Math.atan(learning_rate_index) / (Math.PI/2)));
			
			
			while(tuple_ptr < tuple_count) {
				
				//System.out.print("Incremental Update");
				
				if(!just_trained && (tuple_ptr % incremental_update_freq) == 0) {
					
					
					
					double current_accuracy = local_correct_count / (double) local_count * 100;
					
					//System.out.println("accuracy: " + local_correct_count / (double) local_count * 100 + ", learning rate index: " + learning_rate_index + " " + ", learning rate: " + Math.abs(Math.atan(learning_rate_index) / (Math.PI/2)));

					local_correct_count = 0;
					local_count = 0;
					
					
					if(last_accuracy != Double.NaN) {
						if(current_accuracy < 60) learning_rate_index = Math.tan(Math.PI/32); //decrease_learning_rate();
						else decrease_learning_rate();
					}
					
					local_learning_rate.add(Math.abs(Math.atan(learning_rate_index) / (Math.PI/2)));

					
					last_accuracy = current_accuracy;
					
					
					old_tuples = training_tuples.map(new Function<Tuple3<Double,String,String>, Tuple3<Double,String,String>>() {

						@Override
						public Tuple3<Double, String, String> call(
								Tuple3<Double, String, String> v1)
								throws Exception {
							return new Tuple3<Double, String, String> (v1._1()*(1.0 - get_learning_rate()), v1._2(), v1._3());
						}

						private double get_learning_rate() {
							
							return Math.abs(Math.atan(learning_rate_index) / (Math.PI/2));
						}
					});
					
					training_tuples = sc.parallelize(data_tuples_at_the_driver.subList(tuple_ptr - incremental_update_freq, tuple_ptr)).map(new Function<Tuple3<Long,String,String>, Tuple3<Double,String,String>>() {

						@Override
						public Tuple3<Double, String, String> call(
								Tuple3<Long, String, String> v1) throws Exception {
							return new Tuple3<Double, String, String>(1.0, v1._2(), v1._3());
						}
					});
					
					training_tuples = old_tuples.union(training_tuples);
					
					training_lps = training_tuples.map(new Function<Tuple3<Double,String,String>, LabeledPoint>() {
						
						@Override
				    	public LabeledPoint call(
				    			Tuple3<Double, String, String> t) throws Exception {
				    		
				    		Double label_rep = Label.labels[Label.label_map.get(t._3())];
				    		String[] terms = t._2().replaceAll("[^0-9A-Za-z]", " ").split("\\s+", -1);
				    		
				    		HashMap<Integer, Double> map = new HashMap<Integer, Double>();
				    		
				    		HashMap<String, Integer> dict = broadcast_dict.value();
				    		
				    		for(String term : terms) {
				    			
				    			if(term.isEmpty()) continue;
				    			if(stop_words.value().is(term)) continue;
				    			
				    			Double value = map.get(dict.get(term));
				    			if(value == null) map.put(dict.get(term), t._1());
				    			else map.put(dict.get(term), value +  t._1());
				    		
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
				    		
				    		return lp;
				    	}
					});
				
					nbm = NaiveBayes.train(training_lps.rdd());
					
					just_trained = true;
					
				}
				else {
					//System.out.println("Predicting");
					Tuple3<Long, String, String> t = data_tuples_at_the_driver.get(tuple_ptr);
					
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
						local_correct_count++;
						//decrease_learning_rate();
						predictions.add(new Tuple3<Long, Integer, Boolean>(t._1(), tuple_ptr, true));
					}
					else {
						//increase_learning_rate();
						predictions.add(new Tuple3<Long, Integer, Boolean>(t._1(), tuple_ptr, false));
					}
					
					tuple_ptr++;
					
					local_count++;
					
					just_trained = false;

				}		
				
			}
			
			
			
			Double cumulative_accuraccy = 0.0;
			
			int total_correct_count = 0;
			int total_count = 0;
			
			PrintStream ps = null;
			try {
				ps = new PrintStream(new File("./cumulative_error_incremental.txt"));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			int ctr2 = 0, ctr3 = 0;
			for(Tuple3<Long, Integer, Boolean> pred : predictions) {
				if(pred._3()) total_correct_count++;
				total_count++;
				
				//System.out.println(pred._1() + " " + pred._2() + " " + (total_correct_count/(double) total_count) * 100);
				
				
				
				ps.println(pred._1() + "	" + pred._2() + "	" + (total_correct_count/(double) total_count) * 100 + "	" + local_learning_rate.get((int) Math.floor(ctr2 / (double) incremental_update_freq)));
				ctr2++;
				
			}
			
			
			//System.out.println("total average accuracy: " + total_correct_count / (double) total_count * 100);
			

					
					
	    }

		private static void increase_learning_rate() {
			if(learning_rate_index > 0.0) learning_rate_index += (0.5);
			else learning_rate_index -= (0.5);
			
		}

		private static void decrease_learning_rate() {
			if(learning_rate_index > 0.0) learning_rate_index -= (0.01);
			else learning_rate_index += (0.01);
		}
	    
	    
		
	}

