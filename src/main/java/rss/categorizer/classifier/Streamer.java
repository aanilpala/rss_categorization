package rss.categorizer.classifier;


import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.base.Optional;

import rss.categorizer.config.Time;
import rss.categorizer.model.*;
import rss.categorizer.model.Dictionary;
import scala.Tuple2;
import scala.Tuple3;


public class Streamer {

	//public static Long window_duration = 2*TimeConversion.an_hour;
	//public static Long sliding_interval= 2*TimeConversion.an_hour;
	public static Long batch_interval = 6*Time.an_hour;
	
	public static Long training_dur = 6*Time.an_hour;
	public static Long training_interval = 24*Time.an_hour;
	
	public static void main(String[] args) {
		
	
		
		SparkConf conf = new SparkConf().setAppName("streamer-playaround").setMaster("local[4]");
			
		
		// disabling default verbouse mode of the loggers
		Logger.getLogger("org").setLevel(Level.FATAL);
	    Logger.getLogger("akka").setLevel(Level.FATAL); 
	   
		
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(batch_interval));  //batch interval discretizes the continous input
		
	    ssc.checkpoint("/tmp/spark/checkpoint");
	    	    
	    JavaReceiverInputDStream<String> stream = ssc.socketTextStream("localhost", 9999);

	    
	    
	
	    
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
				if((tuple._1()/Time.scaling_factor % training_interval) < training_dur) return true;
				else return false;
				//return false;
			}
	    	
	    	
		}).flatMapToPair(new PairFlatMapFunction<Tuple3<Long,String,String>, Integer, DictionaryEntry>() {

			@Override
			public Iterable<Tuple2<Integer, DictionaryEntry>> call(Tuple3<Long, String, String> t)
					throws Exception {

				List<Tuple2<Integer, DictionaryEntry>> bag = new ArrayList<Tuple2<Integer, DictionaryEntry>>();
				String[] terms = t._3().toLowerCase().trim().replaceAll("[^0-9A-Za-z]", " ").split(" ");
				
				Set<String> uniqueTerms = new HashSet<String>(Arrays.asList(terms)); // we don't need tf for the dictionary

				Integer batch_num = (int) ((Math.floor((t._1() - Time.start_point) / (training_interval*Time.scaling_factor))));
				
				for(String term : uniqueTerms) {
					if(term.length() == 0) continue;
					
					DictionaryEntry dictionaryEntry = new DictionaryEntry(term, batch_num);
					Tuple2<Integer, DictionaryEntry> dictionaryStreamEntry = new Tuple2<Integer, DictionaryEntry>(dictionaryEntry.getIndex(), dictionaryEntry);
					
					bag.add(dictionaryStreamEntry);
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

/*
		tuple_stream.foreachRDD(new Function<JavaRDD<Tuple3<Long, String, String>>, Void>() {
			@Override
			public Void call(JavaRDD<Tuple3<Long, String, String>> inputElement) throws Exception {

				List<Tuple3<Long, String, String>> streamTuples = inputElement.collect();

				if (!streamTuples.isEmpty()) {

					for(Tuple3<Long, String, String> t : streamTuples){
						String category = t._2();
						String text = t._3();

						String[] tokens = text.split("\\s+");

					}

				}


				return null;
			}
		});
*/
	     
	   
//	    JavaDStream<Tuple2<Integer, DictionaryEntry>> test = accumulated_dictionary_stream.map(new Function<Tuple2<Integer,DictionaryEntry>, Tuple2<Integer,DictionaryEntry>>() {
//
//			@Override
//			public Tuple2<Integer, DictionaryEntry> call(
//					Tuple2<Integer, DictionaryEntry> v1) throws Exception {
//				// TODO Auto-generated method stub
//				return v1;
//			}
//		});
//	    
//	    
//	
//	    
//	  
//	  
//	    JavaDStream<Tuple2<String, List<Integer>>> test2 = tuple_stream.transformWith(test, new func3()); 
//	    
//	    test2.print();
	    

		//for joining on hashcode construct stream of <key= hashcode,<timestamp, category>>
		JavaPairDStream<Integer, Long> hash_keys = tuple_stream.flatMapToPair(new PairFlatMapFunction<Tuple3<Long, String, String>, Integer, Long>() {
			@Override
			public Iterable<Tuple2<Integer, Long>> call(final Tuple3<Long, String, String> tuple_stream_item) throws Exception {


				List<Tuple2<Integer, Long>> hashkeys = new ArrayList<Tuple2<Integer, Long>>();
				String[] tokens = tuple_stream_item._3().split("\\s+");
				Long timestamp = tuple_stream_item._1();
				
				for(String w: tokens){
					w.toLowerCase().replaceAll("[^\\dA-Za-z]", "").trim();
					int index = w.hashCode();
					hashkeys.add(new Tuple2<Integer, Long>(index, timestamp));
				}

				return hashkeys;
			}
		});

		
		JavaPairDStream<Integer, Tuple2<Long, DictionaryEntry>> unfolded_data_points = hash_keys.join(accumulated_dictionary_stream);
		
		JavaPairDStream<Long, List<Tuple2<Integer, Integer>>> projected_unfolded_data_points = unfolded_data_points.mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Long,DictionaryEntry>>, Long, List<Tuple2<Integer, Integer>>>() {

			@Override
			public Tuple2<Long, List<Tuple2<Integer, Integer>>> call(
					Tuple2<Integer, Tuple2<Long, DictionaryEntry>> v1)
					throws Exception {
				// TODO Auto-generated method stub
				
				List<Tuple2<Integer, Integer>> term_array = new ArrayList<Tuple2<Integer, Integer>>();
				term_array.add(new Tuple2<Integer, Integer>(v1._1, v1._2._2.getTotalDf())); // getting total df, this is only for incremental approach
				return new Tuple2<Long, List<Tuple2<Integer, Integer>>>(v1._2._1, term_array);
			}
			
		});
		
		
		JavaPairDStream<Long, List<Tuple2<Integer, Integer>>> labeled_points = projected_unfolded_data_points.reduceByKey(new Function2<List<Tuple2<Integer,Integer>>, List<Tuple2<Integer,Integer>>, List<Tuple2<Integer,Integer>>>() {

			@Override
			public List<Tuple2<Integer, Integer>> call(
					List<Tuple2<Integer, Integer>> v1,
					List<Tuple2<Integer, Integer>> v2) throws Exception {
				
				v1.addAll(v2);
				return v1;
				
			}
		});
		
		
		labeled_points.foreachRDD(new Function<JavaPairRDD<Long,List<Tuple2<Integer,Integer>>>, Void>() {
			
			@Override
			public Void call(JavaPairRDD<Long, List<Tuple2<Integer, Integer>>> v1)
					throws Exception {
				
				
				v1.foreach(new VoidFunction<Tuple2<Long,List<Tuple2<Integer,Integer>>>>() {

					@Override
					public void call(
							Tuple2<Long, List<Tuple2<Integer, Integer>>> t)
							throws Exception {
						
						System.out.print(t._1 + "-" );
						
						for(Tuple2<Integer, Integer> each : t._2) {
							
							System.out.print(each._1 + " : " + each._2 + ", ");
						}
							
						System.out.println();
					}
				});
				
				return null;
				
			}
		});
		
		
		//labeled_points.print();
		accumulated_dictionary_stream.print();
		//  dictionary_stream.print();
		// tuple_stream.print();
		ssc.start();
	    ssc.awaitTermination();
	    
	}
	
	
}
