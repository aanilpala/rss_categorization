package classifier;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import scala.Tuple2;
import scala.Tuple3;
import feed.model.DictionaryEntry;


public class Streamer {

	public static Long window_duration = 3600L; // this corresponds to an hour
	public static Long sliding_interval= 3600L; 
	public static Long batch_interval = 3600L; 
	
	//public static Long training_interval = 3000L*60;
	
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
				
				String[] tokens = item.split(",");
				return new Tuple3<Long, String, String>(Long.parseLong(tokens[0].substring(1, tokens[0].length())), tokens[2], tokens[1]);
				
			}
		});
	    
	    
	    
	    JavaPairDStream<Integer, DictionaryEntry> raw_dictionary_stream = tuple_stream.filter(new Function<Tuple3<Long, String,String>, Boolean>() {

			@Override
			public Boolean call(Tuple3<Long, String,String> tuple) throws Exception {
				if((tuple._1() % 24*60*60*1000) < 6*60*60*1000) return true;
				else return false;
			}
	    	
	    	
		}).flatMapToPair(new PairFlatMapFunction<Tuple3<Long,String,String>, Integer, DictionaryEntry>() {

			@Override
			public Iterable<Tuple2<Integer, DictionaryEntry>> call(Tuple3<Long, String, String> t)
					throws Exception {

				List<Tuple2<Integer, DictionaryEntry>> bag = new ArrayList<Tuple2<Integer, DictionaryEntry>>();
				String[] terms = t._3().split(" ");
				
				Set<String> uniqueTerms = new HashSet<String>(Arrays.asList(terms)); // we don't need tf for the dictionary

				
				for(String term : uniqueTerms) {
					String dicTerm = term.toLowerCase().replaceAll("[^\\dA-Za-z]| ", "").trim();
					DictionaryEntry dictionaryEntry = new DictionaryEntry(term);
					Tuple2<Integer, DictionaryEntry> dictionaryStreamEntry = new Tuple2<Integer, DictionaryEntry>(dictionaryEntry.getIndex(), dictionaryEntry);
					
					bag.add(dictionaryStreamEntry);
				}
				
				return bag;
			}
			
		});
	    
	    
	    JavaPairDStream<Integer, DictionaryEntry> reduced_dictionary_stream = raw_dictionary_stream.reduceByKey(new Function2<DictionaryEntry, DictionaryEntry, DictionaryEntry>() {

			@Override
			public DictionaryEntry call(DictionaryEntry v1, DictionaryEntry v2)
					throws Exception {
				v1.incrementDf();  // we are sure that the same terms cannot be from the same item/doc so we can increment df!
				return v1;
			}
		});
	    
//	    final ArrayList<DictionaryEntry> temp = new ArrayList<DictionaryEntry>();
//	    
//	    raw_dictionary_stream.foreachRDD(new Function<JavaPairRDD<Integer,DictionaryEntry>, Void>() {
//
//			@Override
//			public Void call(JavaPairRDD<Integer, DictionaryEntry> v1)
//					throws Exception {
//				v1.foreach(new VoidFunction<Tuple2<Integer,DictionaryEntry>> () {
//
//					@Override
//					public void call(Tuple2<Integer, DictionaryEntry> t)
//							throws Exception {
//						temp.add(t._2);
//						
//					}
//					
//				});
//				return null;
//			}
//	    	
//	    	
//		});
//	    
//	    
//	    for(DictionaryEntry each : temp) {
//	    	System.out.println(each);
//	    }
	    
	    reduced_dictionary_stream.print();
	    //tuple_stream.print();
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
	
	Function2<Integer, Integer, Integer> windowReducer = new Function2<Integer, Integer, Integer>() {
		  @Override public Integer call(Integer i1, Integer i2) throws Exception {
		    return i1 + i2;
		  }
		};
	
}
