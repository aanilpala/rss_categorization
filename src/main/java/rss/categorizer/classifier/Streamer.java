package rss.categorizer.classifier;


import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
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
		// for joining on hashcode construct stream of <key= hashcode,<timestamp, category>>
		JavaPairDStream<Integer, Tuple2<Long, String>> hashText = tuple_stream.flatMapToPair(new PairFlatMapFunction<Tuple3<Long, String, String>, Integer, Tuple2<Long, String>>() {
			@Override
			public Iterable<Tuple2<Integer,Tuple2<Long, String>>> call(final Tuple3<Long, String, String> streamTuples) throws Exception {


				List<Tuple2<Integer,Tuple2<Long, String>>> results = new ArrayList<Tuple2<Integer, Tuple2<Long, String>>>();
				String[] tokens = streamTuples._3().split("\\s+");
				for(String w: tokens){
					w.toLowerCase().replaceAll("[^\\dA-Za-z]", "").trim();
					int index = w.hashCode();
					Tuple2<Long, String> timestampCateg = new Tuple2<Long, String>(streamTuples._1(), streamTuples._2());
					results.add(new Tuple2<Integer, Tuple2<Long, String>>(index,timestampCateg));
				}

				return results;
			}
		});

		// join to <hashcode,<<timestamp, category>, DictionaryEntry>>
		JavaPairDStream<Integer,Tuple2<Tuple2<Long, String>, DictionaryEntry>> joinedWithDic = hashText.join(accumulated_dictionary_stream);
		//joinedWithDic.print();

		// make timestamp+hashcode insert 1 in order to reduce frequencies in next step
		JavaPairDStream<String, Tuple3<Integer, String, DictionaryEntry>> reordered = joinedWithDic.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Tuple2<Long, String>, DictionaryEntry>>, String, Tuple3<Integer, String, DictionaryEntry>>() {
			@Override
			public Iterable<Tuple2<String, Tuple3<Integer, String, DictionaryEntry>>> call(Tuple2<Integer, Tuple2<Tuple2<Long, String>, DictionaryEntry>> input) throws Exception {
				Long timestamp = input._2()._1()._1();
				String category = input._2()._1._2();
				DictionaryEntry dicEntry = input._2()._2();
				Integer index = input._1();
				Tuple3<Integer, String, DictionaryEntry> value = new Tuple3<Integer, String, DictionaryEntry>(1,category,dicEntry);
				List<Tuple2<String,Tuple3<Integer, String, DictionaryEntry>>> results = new ArrayList<Tuple2<String, Tuple3<Integer, String, DictionaryEntry>>>();
				String key = timestamp.toString() + ":" + index.toString();
				results.add(new Tuple2<String, Tuple3<Integer, String, DictionaryEntry>>(key,value));
				return results;
			}
		});

		//reordered.print();



		JavaPairDStream<String,Tuple3<Integer, String, DictionaryEntry>> wordCountsStream = reordered.reduceByKey(new Function2<Tuple3<Integer, String, DictionaryEntry>, Tuple3<Integer, String, DictionaryEntry>, Tuple3<Integer, String, DictionaryEntry>>() {
			@Override
			public Tuple3<Integer, String, DictionaryEntry> call(Tuple3<Integer, String, DictionaryEntry> val1, Tuple3<Integer, String, DictionaryEntry> val2) throws Exception {

				int wordcount = val1._1() + val2._1();
				// TODO: tf-idf or similar
				Tuple3<Integer, String, DictionaryEntry> result = new Tuple3<Integer, String, DictionaryEntry>(wordcount, val1._2(),val1._3());
				return result;
			}

		});

		wordCountsStream.print();


/*





		hashText.foreachRDD(new Function<JavaPairRDD<Integer, String>, Void>() {
			@Override
			public Void call(JavaPairRDD<Integer, String> pair) throws Exception {
				System.out.println(pair.first());
				return null;
			}
		});
*/








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

				//  accumulated_dictionary_stream.print();
				//  dictionary_stream.print();
				// tuple_stream.print();
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
