package rss.categorizer.classifier;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.base.Optional;

import rss.categorizer.config.Time;
import rss.categorizer.model.DictionaryEntry;
import scala.Tuple2;
import scala.Tuple3;


public class Streamer {

	//public static Long window_duration = 2*TimeConversion.an_hour;
	//public static Long sliding_interval= 2*TimeConversion.an_hour;
	public static Long batch_interval = 6*Time.an_hour;
	
	public static Long training_dur = 6*Time.an_hour;
	public static Long training_interval = 24*Time.an_hour;
	
	public static void main(String[] args) {
		
	
		
		SparkConf conf = new SparkConf().setAppName("streamer-playaround").setMaster("local[2]");
			
		
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
	    
	    
	    
//	    accumulated_dictionary_stream.foreachRDD(new Function<JavaPairRDD<Integer,DictionaryEntry>, Void>() {
//
//			@Override
//			public Void call(JavaPairRDD<Integer, DictionaryEntry> v1)
//					throws Exception {
//				List<Tuple2<Integer, DictionaryEntry>> temp = v1.collect();
//
//				System.out.println(temp.size());
//				
////				for(Tuple2<Integer, DictionaryEntry> each : temp) {
////					System.out.println(each._2.toString());
////				}
//				
//				return null;
//				
//				}
//	    });
//	    
//	    dictionary_stream.foreachRDD(new Function<JavaPairRDD<Integer,DictionaryEntry>, Void>() {
//
//			@Override
//			public Void call(JavaPairRDD<Integer, DictionaryEntry> v1)
//					throws Exception {
//				List<Tuple2<Integer, DictionaryEntry>> temp = v1.collect();
//				
//				System.out.println(temp.size());
//				
//				return null;
//				
//			}
//	    	
//	    });
	    
	    accumulated_dictionary_stream.print();
	    dictionary_stream.print();
	    tuple_stream.print();
	    ssc.start();
	    ssc.awaitTermination();
	   
	   
	    
	    
//	    Queue<JavaRDD<FeedItem>> rddQueue = new LinkedList<JavaRDD<FeedItem>>();
//
//	    List<FeedItem> list = new ArrayList<FeedItem>();
//	    
//	   
//	    for (int i = 0; i < 30; i++) {
//	      rddQueue.add(ssc.sparkContext().parallelize(list));
//	    }	    
//	    
//	    ssc.queueStream(queue, oneAtATime)
	    
	}
	
	
}
