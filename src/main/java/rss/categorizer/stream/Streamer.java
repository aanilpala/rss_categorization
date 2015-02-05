package rss.categorizer.stream;


import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.base.Optional;

import rss.categorizer.config.Time;
import rss.categorizer.model.*;
import rss.categorizer.util.TFIDF;
import scala.Tuple2;
import scala.Tuple3;


public class Streamer {

	//public static Long window_duration = 2*TimeConversion.an_hour;
	//public static Long sliding_interval= 2*TimeConversion.an_hour;
	private static Long batch_interval = 6*Time.an_hour;
	
	private static Long training_dur = 6*Time.an_hour;
	private static Long training_interval = 24*Time.an_hour;
	
	private static boolean incremental_update = true;
	
	private static final NaiveBayesianWrapper nb = new NaiveBayesianWrapper(incremental_update); 

	public static final String  HADOOP_SECURITY_GROUPS_CACHE_SECS = "hadoop.security.groups.cache.secs"; 
	
	public static void main(String[] args) {
		
	
		
		SparkConf conf = new SparkConf().setAppName("streamer").setMaster("local[2]");
		SparkConf conf2 = new SparkConf().setAppName("classifier").setMaster("local[1]");
		
		// disabling default verbouse mode of the loggers
		Logger.getLogger("org").setLevel(Level.FATAL);
	    Logger.getLogger("akka").setLevel(Level.FATAL); 
	    
	    conf.set("spark.driver.allowMultipleContexts", "true");
	    conf2.set("spark.driver.allowMultipleContexts", "true");
	    
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(batch_interval));  //batch interval discretizes the continous input
		JavaSparkContext sc = new JavaSparkContext(conf2);
		
	    ssc.checkpoint("/tmp/spark/checkpoint");
	    	    
	    JavaReceiverInputDStream<String> stream = ssc.socketTextStream("localhost", 9999);

	    nb.setSC(sc);
	    
	
	    
	    // String to Tuple3 Conversion
	    JavaDStream<Tuple3<Long, String, String>> tuple_stream = stream.map(new Function<String, Tuple3<Long, String, String>>() {

			@Override
			public Tuple3<Long, String, String> call(String item) throws Exception {
				
				String[] tokens = item.replaceAll("\\(|\\)", "").split(",");
				return new Tuple3<Long, String, String>(Long.parseLong(tokens[0]), tokens[2], tokens[1]);
				
			}
		});
	    
	    
	    JavaPairDStream<Integer, DictionaryEntry> raw_dictionary_stream = tuple_stream.filter(new Function<Tuple3<Long, String,String>, Boolean>() {

			@Override
			public Boolean call(Tuple3<Long, String,String> tuple) throws Exception {
				//if((tuple._1()/Time.scaling_factor % training_interval) < training_dur) return true;
				//else return false;
				return true;
			}
	    	
	    	
		}).flatMapToPair(new PairFlatMapFunction<Tuple3<Long,String,String>, Integer, DictionaryEntry>() {

			@Override
			public Iterable<Tuple2<Integer, DictionaryEntry>> call(Tuple3<Long, String, String> t)
					throws Exception {

				List<Tuple2<Integer, DictionaryEntry>> bag = new ArrayList<Tuple2<Integer, DictionaryEntry>>();
				String[] terms = t._3().toLowerCase().trim().replaceAll("[^0-9A-Za-z]", " ").split("\\s+");
				
				Set<String> uniqueTerms = new HashSet<String>(Arrays.asList(terms)); // we don't need tf for the dictionary

				Integer batch_num = (int) ((Math.floor((t._1() - Time.start_point) / (training_interval*Time.scaling_factor))));
				
				for(String term : uniqueTerms) {
					DictionaryEntry dictionaryEntry = new DictionaryEntry(term, batch_num);
					Tuple2<Integer, DictionaryEntry> dictionaryStreamEntry = new Tuple2<Integer, DictionaryEntry>(dictionaryEntry.getIndex(), dictionaryEntry);
					
					bag.add(dictionaryStreamEntry);
					nb.insert_into_dictionary_index(term.hashCode());
				}
				
				return bag;
			}
			
		});
	    
	    
	    
	    
	    JavaPairDStream<Integer, DictionaryEntry> dictionary_stream = raw_dictionary_stream.reduceByKey(new Function2<DictionaryEntry, DictionaryEntry, DictionaryEntry>() {

			@Override
			public DictionaryEntry call(DictionaryEntry v1, DictionaryEntry v2)
					throws Exception {
				v1.incrementDf();  // we are sure that the same terms cannot be from the same item/doc so we can increment df!
				return v1;
			}
		});
	    
//	    int cur_batch_num = 0;
	    JavaPairDStream<Integer, DictionaryEntry> accumulated_dictionary_stream = dictionary_stream.updateStateByKey(new Function2<List<DictionaryEntry>, Optional<DictionaryEntry>, Optional<DictionaryEntry>>() {

			@Override
			public Optional<DictionaryEntry> call(List<DictionaryEntry> new_entries,
					Optional<DictionaryEntry> state) throws Exception {
				
				if(state.isPresent()) {
					
					if(new_entries.size() == 0) return state;
					
					else {
						DictionaryEntry de = state.get();
						de.updateBatch(new_entries.get(0).getBatchNumber(), new_entries.size()); 
						return Optional.of(de);
					}
					
					
				}
				else {
					DictionaryEntry de = new_entries.get(0);
					de.updateBatch(new_entries.get(0).getBatchNumber(), new_entries.size()-1); 
					return Optional.of(de);
				}
				
				
			}			
		});
	    

		// hashindex, label, timestamp
		JavaPairDStream<Integer, Tuple2<Integer, Long>> hash_keys = tuple_stream.flatMapToPair(new PairFlatMapFunction<Tuple3<Long, String, String>, Integer, Tuple2<Integer, Long>>() {
			@Override
			public Iterable<Tuple2<Integer, Tuple2<Integer, Long>>> call(final Tuple3<Long, String, String> tuple_stream_item) throws Exception {


				List<Tuple2<Integer, Tuple2<Integer, Long>>> hashkeys = new ArrayList<Tuple2<Integer, Tuple2<Integer, Long>>>();
				String[] tokens = tuple_stream_item._3().toLowerCase().trim().replaceAll("[^0-9A-Za-z]", " ").split("\\s+");
				Long timestamp = tuple_stream_item._1();
				Integer label = Label.label_map.get(tuple_stream_item._2()); 
				
				for(String w: tokens){
					int index = w.hashCode();
					hashkeys.add(new Tuple2<Integer, Tuple2<Integer, Long>>(index, new Tuple2<Integer, Long>(label, timestamp)));
				}

				return hashkeys;
			}
		});

		
		JavaPairDStream<Integer, Tuple2<Tuple2<Integer, Long>, DictionaryEntry>> unfolded_data_points = hash_keys.join(accumulated_dictionary_stream);
		
		
	
		JavaPairDStream<Tuple3<Long, Integer, Integer>, Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>>> projected_twice_unfolded_data_points = unfolded_data_points.mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Tuple2<Integer,Long>,DictionaryEntry>>, Tuple3<Long, Integer, Integer>, Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>>>() {

			@Override
			public Tuple2<Tuple3<Long, Integer, Integer>, Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>>> call(
					Tuple2<Integer, Tuple2<Tuple2<Integer, Long>, DictionaryEntry>> t)
					throws Exception {
				List<Tuple3<Integer, Integer, Integer>> term_array = new ArrayList<Tuple3<Integer, Integer, Integer>>();
				term_array.add(new Tuple3<Integer, Integer, Integer>(t._1, t._2._2.getTotalDf(), 1));
				Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>> wrapped_term_array = new Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>>(t._1, term_array);
				return new Tuple2<Tuple3<Long, Integer, Integer>, Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>>>(new Tuple3<Long, Integer, Integer>(t._2._1._2, t._2._1._1, t._1), wrapped_term_array);

			}
			
		});
		
		
		JavaPairDStream<Tuple2<Long, Integer>, Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>>> projected_unfolded_data_points = projected_twice_unfolded_data_points.reduceByKey(new Function2<Tuple2<Integer,List<Tuple3<Integer,Integer,Integer>>>, Tuple2<Integer,List<Tuple3<Integer,Integer,Integer>>>, Tuple2<Integer,List<Tuple3<Integer,Integer,Integer>>>>() {

			@Override
			public Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>> call(
					Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>> v1,
					Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>> v2)
					throws Exception {
				
				Integer index = v1._2.get(0)._1();
				Integer df = v1._2.get(0)._2();
				Integer tf = v1._2.get(0)._3() + v2._2.get(0)._3(); 
				
				Tuple3<Integer, Integer, Integer> aggregated_tuple = new Tuple3<Integer, Integer, Integer>(index, df, tf);
	
				return new Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>>(v1._1, new ArrayList<Tuple3<Integer, Integer, Integer>>(Arrays.asList(aggregated_tuple)));
			}
		}).mapToPair(new PairFunction<Tuple2<Tuple3<Long,Integer,Integer>,Tuple2<Integer,List<Tuple3<Integer,Integer,Integer>>>>, Tuple2<Long, Integer>, Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>>>() {

			@Override
			public Tuple2<Tuple2<Long, Integer>, Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>>> call(
					Tuple2<Tuple3<Long, Integer, Integer>, Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>>> t)
					throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Tuple2<Long, Integer>, Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>>>(new Tuple2<Long, Integer>(t._1._1(), t._1._2()), t._2);
			}
		});
		
		
		JavaPairDStream<Tuple2<Long, Integer>, Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>>> labeled_points = projected_unfolded_data_points.reduceByKey(new Function2<Tuple2<Integer,List<Tuple3<Integer,Integer,Integer>>>, Tuple2<Integer,List<Tuple3<Integer,Integer,Integer>>>, Tuple2<Integer,List<Tuple3<Integer,Integer,Integer>>>>() {

			@Override
			public Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>> call(
					Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>> v1,
					Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>> v2)
					throws Exception {
				// TODO Auto-generated method stub
				v1._2.addAll(v2._2);
				return v1;
			}
			
		});
		
		
		accumulated_dictionary_stream.foreach(new Function<JavaPairRDD<Integer,DictionaryEntry>, Void>() {

			@Override
			public Void call(JavaPairRDD<Integer, DictionaryEntry> v1)
					throws Exception {
				
				if(!v1.collect().isEmpty())
					nb.insert_into_dictionary_index(v1.collect().get(0)._2().getIndex());
				
				return null;
			}
			
			
			
		});
		
		
		
		labeled_points.foreachRDD(new Function<JavaPairRDD<Tuple2<Long, Integer>,Tuple2<Integer,List<Tuple3<Integer, Integer, Integer>>>>, Void>() {

			@Override
			public Void call(
					JavaPairRDD<Tuple2<Long, Integer>, Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>>> v1)
					throws Exception {
			
				
				
				v1.foreach(new VoidFunction<Tuple2<Tuple2<Long, Integer>, Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>>>>() {
					
					@Override
					public void call(
							Tuple2<Tuple2<Long, Integer>, Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>>> t)
							throws Exception {
					
						System.out.print(t._2._1 + " -> " );
						
						for(Tuple3<Integer, Integer, Integer> each : t._2._2) {
							
							System.out.print(each._1() + " : " + each._2() + ", "  + each._3() + ", " );
						}
							
						System.out.println();
						
				
					}

					
				});
				return null;
			

		}});
		
		
		
//		
//		accumulated_dictionary_stream.foreachRDD(new Function<JavaPairRDD<Integer,DictionaryEntry>, Void>() {
//
//			@Override
//			public Void call(JavaPairRDD<Integer, DictionaryEntry> v1)
//					throws Exception {
//				
//				v1.foreach(new VoidFunction<Tuple2<Integer,DictionaryEntry>>() {
//
//					@Override
//					public void call(Tuple2<Integer, DictionaryEntry> t)
//							throws Exception {
//						
//						System.out.println(t._2.toString());
//						
//					}
//					
//					
//				});
//				
//				return null;
//			}
//		});
		
		
		
		labeled_points.foreachRDD(new Function<JavaPairRDD<Tuple2<Long, Integer>, Tuple2<Integer,List<Tuple3<Integer, Integer,Integer>>>>, Void>() {

			@Override
			public Void call(
					JavaPairRDD<Tuple2<Long, Integer>, Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>>> v1)
					throws Exception {
				
				if(v1.collect().isEmpty()) return null;
				Tuple2<Tuple2<Long, Integer>, Tuple2<Integer, List<Tuple3<Integer, Integer, Integer>>>> rdd = v1.collect().get(0);
				
				
				
				System.out.println("STATE TEST: " + nb.getState());
				
				if(nb.getState() == 0) nb.setState(1);
				else if(nb.getState() == 1){
					if((rdd._1._1/Time.scaling_factor % training_interval) > training_dur) {
						nb.prepareModel();
						nb.setState(2);
					}
				}
				else if(nb.getState() == 2) {
					if((rdd._1._1/Time.scaling_factor % training_interval) < training_dur) {
						nb.resetTrainingSession();
						nb.setState(1);
					}
					else; // keep predicting
				}
				
				
					
				List<Tuple3<Integer, Integer, Integer>> index_feature_list = rdd._2._2;
				int indices_array[] = new int[index_feature_list.size()];
				double features_list[] = new double[index_feature_list.size()];
					
				Integer max_tf = 0;
				int ctr = 0;
				for(Tuple3<Integer, Integer, Integer> index_feature_item : index_feature_list) {
					indices_array[ctr++] = (index_feature_item._1());
					if(max_tf < index_feature_item._3()) max_tf = index_feature_item._3();
				}
					
				ctr = 0;
				for(Tuple3<Integer, Integer, Integer> index_feature_item : index_feature_list) {
					features_list[ctr++] = TFIDF.computeTFIDF(nb.getDictionarySize(), index_feature_item._2(), index_feature_item._3(), max_tf);
				}
					
				Arrays.sort(indices_array);
				Arrays.sort(features_list);
					
				SparseVector features = new SparseVector(nb.getDictionarySize(), indices_array, features_list);
				LabeledPoint labeled_point = new LabeledPoint((double) rdd._1._2, features);
				
				
				
				if(nb.getState() == 1) nb.insert_into_training_set(labeled_point);
				else if(nb.getState() == 2) {
					double prediction = nb.predict(features);
					System.out.println("prediction: " + prediction + " - label: " + rdd._1._2);
				}
				
				return null;
				
				
			}
			
		});
		
//		
//		labeled_points.foreachRDD(new Function<JavaPairRDD<Long,List<Tuple2<Integer,Integer>>>, Void>() {
//
//			@Override
//			public Void call(
//					JavaPairRDD<Long, List<Tuple2<Integer, Integer>>> v1)
//					throws Exception {
//				
//				a.add(new Integer(1));
//				System.out.println("STATE TEST " + a.size());
//
//				return null;
//			}
//			
//		});
		
		
		
		
		
		//labeled_points.print();
		//accumulated_dictionary_stream.print();
		//dictionary_stream.print();
		
		tuple_stream.print();
		ssc.start();
	    ssc.awaitTermination();
	    
	}
	
	
}
