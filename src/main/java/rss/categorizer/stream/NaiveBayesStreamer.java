package rss.categorizer.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import rss.categorizer.config.Time;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.reflect.internal.Symbols.TermSymbol;

public class NaiveBayesStreamer {
	
	private static Long batch_interval = 5*24*Time.an_hour_scaled;
	
	private static Long training_dur = 5*24*Time.an_hour_scaled;
	private static Long training_interval = 7*24*Time.an_hour_scaled;
	
	private static boolean incremental_update = true;
	
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("streamer").setMaster("local[2]");
		
		Logger.getLogger("org").setLevel(Level.FATAL);
		Logger.getLogger("akka").setLevel(Level.FATAL);
		
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(batch_interval));  //batch interval discretizes the continous input
	    ssc.checkpoint("/tmp/spark/checkpoint");
	    JavaReceiverInputDStream<String> stream = ssc.socketTextStream("localhost", 9999);
	    

		//final Accumulator<Integer> totalSize = ssc.sc().accumulator(0);
	    
	    
		
	    // String to Tuple3 Conversion
	    JavaDStream<Tuple3<Long, String, String>> tuple_stream = stream.map(new Function<String, Tuple3<Long, String, String>>() {

			@Override
			public Tuple3<Long, String, String> call(String item) throws Exception {
				
				String[] tokens = item.replaceAll("\\(|\\)", "").split(",");
				return new Tuple3<Long, String, String>(Long.parseLong(tokens[0]), tokens[2], tokens[1]);
				
			}
		});
	    
	    // (label, term), count, timestamp
	    JavaPairDStream<Tuple2<Integer, Integer>, Tuple2<Integer, Long>> label_term_count = tuple_stream.flatMap(new FlatMapFunction<Tuple3<Long,String,String>, Tuple3<Integer, Integer, Tuple2<Integer, Long>>>() {

			@Override
			public Iterable<Tuple3<Integer, Integer, Tuple2<Integer, Long>>> call(
					Tuple3<Long, String, String> t) throws Exception {
				// TODO Auto-generated method stub
				String[] terms = t._3().toLowerCase().trim().replaceAll("[^0-9A-Za-z]", " ").split("\\s+");
				
				List<Tuple3<Integer, Integer, Tuple2<Integer, Long>>> out = new ArrayList<Tuple3<Integer, Integer, Tuple2<Integer, Long>>>();
				
				for(int ctr = 0; ctr < terms.length; ctr++) {
					out.add(new Tuple3<Integer, Integer, Tuple2<Integer, Long>>(Label.label_map.get(t._2()), terms[ctr].hashCode(), new Tuple2<Integer, Long>(1, t._1())));
				}
				
				return out;
			}
	    	
		}).mapToPair(new PairFunction<Tuple3<Integer,Integer,Tuple2<Integer, Long>>, Tuple2<Integer, Integer>, Tuple2<Integer, Long>>() {

			@Override
			public Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Long>> call(
					Tuple3<Integer, Integer, Tuple2<Integer, Long>> t) throws Exception {
				
				return new Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Long>>(new Tuple2<Integer, Integer>(t._1(), t._2()), new Tuple2<Integer, Long>(t._3()._1, t._3()._2));
			}
		}).reduceByKey(new Function2<Tuple2<Integer,Long>, Tuple2<Integer,Long>, Tuple2<Integer,Long>>() {

			@Override
			public Tuple2<Integer, Long> call(Tuple2<Integer, Long> v1,
					Tuple2<Integer, Long> v2) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Integer, Long>(v1._1+v2._1, v1._2);
			}
			
		});
	    
	    
	    
	    JavaPairDStream<Tuple2<Integer, Integer>, Tuple2<Integer, Long>> conditionals = label_term_count.updateStateByKey(new Function2<List<Tuple2<Integer, Long>>, Optional<Tuple2<Integer, Long>>, Optional<Tuple2<Integer, Long>>>() {

			@Override
			public Optional<Tuple2<Integer, Long>> call(List<Tuple2<Integer, Long>> new_tuples, Optional<Tuple2<Integer, Long>> state_tuple)
					throws Exception {
				
				if(new_tuples.isEmpty()) return state_tuple;
				if(new_tuples.get(0)._2*Time.scaling_factor % training_interval > training_dur) return state_tuple;
				
				if(!state_tuple.isPresent()) {
					
					if(new_tuples.get(0)._2*Time.scaling_factor % training_interval <= training_dur) {
						Integer state_count = 0;
						for(Tuple2<Integer, Long> new_tuple : new_tuples) {
							state_count += new_tuple._1;
						}
						Tuple2<Integer, Long> updated_state_tuple = new Tuple2<Integer, Long>(state_count, new_tuples.get(0)._2);
						return Optional.of(updated_state_tuple);
					}
					else return Optional.absent();
				}
				
				Integer state_count = state_tuple.get()._1;
				for(Tuple2<Integer, Long> new_tuple : new_tuples) {
					state_count += new_tuple._1;
				}
				
				Tuple2<Integer, Long> updated_state_tuple = new Tuple2<Integer, Long>(state_count, state_tuple.get()._2);
				return Optional.of(updated_state_tuple);
				
			}
		});
	    
	    
	    JavaPairDStream<Integer, Integer> sums = conditionals.mapToPair(new PairFunction<Tuple2<Tuple2<Integer,Integer>,Tuple2<Integer,Long>>, Integer, Integer>() {

			@Override
			public Tuple2<Integer, Integer> call(
					Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Long>> t)
					throws Exception {
				
				return new Tuple2<Integer, Integer>(t._1._1, t._2._1);
			}
	    	
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
			
		});
	    
	    JavaPairDStream<Tuple2<Integer, Integer>, Integer> total = sums.reduce(new Function2<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>, Tuple2<Integer,Integer>>() {

			@Override
			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1,
					Tuple2<Integer, Integer> v2) throws Exception {
				return new Tuple2<Integer, Integer>(-1, v1._2 + v2._2);
			}
		}).mapToPair(new PairFunction<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>, Integer>() {

			@Override
			public Tuple2<Tuple2<Integer, Integer>, Integer> call(
					Tuple2<Integer, Integer> t) throws Exception {
				
				return new Tuple2<Tuple2<Integer, Integer>, Integer>(new Tuple2<Integer, Integer>(-1,-1), t._2);
			}
			
			
		});
	    
	    
	    // (label, term), (count, sum, prior)
	    JavaPairDStream<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>> state = conditionals.mapToPair(new PairFunction<Tuple2<Tuple2<Integer,Integer>,Tuple2<Integer,Long>>, Integer, Tuple2<Integer, Integer>>() {

			@Override
			public Tuple2<Integer, Tuple2<Integer, Integer>> call(
					Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Long>> t)
					throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Integer, Tuple2<Integer, Integer>>(t._1._1, new Tuple2<Integer, Integer>(t._1._2, t._2._1));
			}
		}).join(sums).mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Tuple2<Integer,Integer>,Integer>>, Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>> () {

			@Override
			public Tuple2<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>> call(
					Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>> t)
					throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>>(new Tuple2<Integer, Integer>(t._1, t._2._1._1), new Tuple3<Integer, Integer, Double>(t._2._1._2, t._2._2, (double) t._2._2));
			}
		}).leftOuterJoin(total).mapToPair(new PairFunction<Tuple2<Tuple2<Integer,Integer>,Tuple2<Tuple3<Integer,Integer,Double>,Optional<Integer>>>, Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>>() {

			@Override
			public Tuple2<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>> call(
					Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple3<Integer, Integer, Double>, Optional<Integer>>> t)
					throws Exception {
				
				Tuple3<Integer, Integer, Double> to_update = t._2._1;
				
				Integer sum = 0;
				
				if(t._2._2.isPresent()) 
					sum = t._2._2.get();
				
				Tuple2<Integer, Integer> key = t._1;
				
				return new Tuple2<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>>(key, new Tuple3<Integer, Integer, Double>(to_update._1(), to_update._2(), to_update._3()/sum));
			}
		});
		
		
	    
	    
	    
//	    sums.map(new Function<Tuple2<Integer,Integer>, Void>() {
//
//			@Override
//			public Void call(Tuple2<Integer, Integer> t) throws Exception {
//				
//				  word_sums.value().put(t._1, t._2);
//				return null;
//				
//			}
//	    	
//		}).print();
//	    
//	    conditionals.map(new Function<Tuple2<Tuple2<Integer,Integer>,Tuple2<Integer,Long>>, Void>() {
//
//			@Override
//			public Void call(
//					Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Long>> t)
//					throws Exception {
//				
//				
//				
//				word_counts.value().put(new Tuple2<Integer, Integer>(t._1._1, t._1._2), t._2._1);
//				return null;
//				
//			}
//		}).print();
	    
	    
//	    conditionals.foreach(new Function<JavaPairRDD<Tuple2<Integer,Integer>,Tuple2<Integer,Long>>, Void>() {
//
//			@Override
//			public Void call(
//					JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Long>> t)
//					throws Exception {
//				
//				
//				
//				System.out.println(t.collect().size());
//				
//				return null;
//			}
//	    	
//		});
	    
	    
	    JavaDStream<Tuple3<Long, String, String>> test_stream = tuple_stream.filter(new Function<Tuple3<Long,String,String>, Boolean>() {

			@Override
			public Boolean call(Tuple3<Long, String, String> t)
					throws Exception {
				if(t._1()*Time.scaling_factor % training_interval <= training_dur) return false;
				else {
					//totalSize.add(1);
					return true;
				}
			}
		});
	    
	    
	    // (candidate, term), (timestamp, label)
	    JavaPairDStream<Tuple2<Integer, Integer>, Tuple2<Long, Integer>> test_stream_unfolded = test_stream.flatMapToPair(new PairFlatMapFunction<Tuple3<Long,String,String>, Tuple2<Integer, Integer>, Tuple2<Long, Integer>>() {

			@Override
			public Iterable<Tuple2<Tuple2<Integer, Integer>, Tuple2<Long, Integer>>> call(
					Tuple3<Long, String, String> t) throws Exception {
				
				String[] terms = t._3().toLowerCase().trim().replaceAll("[^0-9A-Za-z]", " ").split("\\s+");
				List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Long, Integer>>> out = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Long, Integer>>>();
				
				
				for(String term : terms) {
					Integer hashCode = term.hashCode();
					for(int ctr = 0; ctr < 6; ctr++) {
						Tuple2<Integer, Integer> index = new Tuple2<Integer, Integer>(ctr, hashCode);
						Tuple2<Long, Integer> value = new Tuple2<Long, Integer>(t._1(), Label.label_map.get(t._2())); 
						out.add(new Tuple2<Tuple2<Integer, Integer>, Tuple2<Long, Integer>>(index, value));
					}
				}
				
				return out;
				
			}
	    	
		});
	    		
	    // (timestamp, candidate), (contribition, label, sum)	
	    JavaPairDStream<Tuple2<Long, Integer>, Tuple3<Double, Integer, Double>> partial_scores = test_stream_unfolded.join(state).mapToPair(new PairFunction<Tuple2<Tuple2<Integer,Integer>,Tuple2<Tuple2<Long,Integer>,Tuple3<Integer,Integer, Double>>>, Tuple2<Long, Integer>, Tuple3<Double, Integer, Double>>() {

			@Override
			public Tuple2<Tuple2<Long, Integer>, Tuple3<Double, Integer, Double>> call(
					Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Long, Integer>, Tuple3<Integer, Integer, Double>>> t)
					throws Exception {
				
				double smoothing = 0.1;
				Double contribition = Math.log((t._2._2._1() + smoothing) / (double) (t._2._2._2() + smoothing));
				
				Tuple2<Long, Integer> index = new Tuple2<Long, Integer>(t._2._1._1, t._1._1);
				Tuple3<Double, Integer, Double> value = new Tuple3<Double, Integer, Double>(contribition, t._2._1._2, 0.0); //  Math.log(t._2._2._3()));
				
				return new Tuple2<Tuple2<Long, Integer>, Tuple3<Double, Integer, Double>>(index, value);
			}

		
	    	
		});
	    
	    
	    
	    JavaPairDStream<Integer, Integer> predictions = partial_scores.reduceByKey(new Function2<Tuple3<Double,Integer, Double>, Tuple3<Double,Integer, Double>, Tuple3<Double,Integer, Double>>() {

			@Override
			public Tuple3<Double, Integer, Double> call(Tuple3<Double, Integer, Double> v1,
					Tuple3<Double, Integer, Double> v2) throws Exception {
				
//				System.out.println(v1._2() + "-" + v2._2());
//				System.out.println(v1._3() + "-" + v2._3());
				
				return new Tuple3<Double, Integer, Double>(v1._1() + v2._1(), v1._2(), v1._3());
			}
	    	
		}).mapToPair(new PairFunction<Tuple2<Tuple2<Long,Integer>,Tuple3<Double,Integer, Double>>, Long, Tuple3<Integer, Double, Integer>>() {

			@Override
			public Tuple2<Long, Tuple3<Integer, Double, Integer>> call(
					Tuple2<Tuple2<Long, Integer>, Tuple3<Double, Integer, Double>> t)
					throws Exception {
				
				return new Tuple2<Long, Tuple3<Integer, Double, Integer>>(t._1._1, new Tuple3<Integer, Double, Integer>(t._1._2, t._2._1() + t._2._3(), t._2._2()));	
			}
		}).reduceByKey(new Function2<Tuple3<Integer,Double,Integer>, Tuple3<Integer,Double,Integer>, Tuple3<Integer,Double,Integer>>() {

			@Override
			public Tuple3<Integer, Double, Integer> call(
					Tuple3<Integer, Double, Integer> v1,
					Tuple3<Integer, Double, Integer> v2) throws Exception {
				
				if(v1._2() > v2._2()) return v1;
				else return v2;
				
			}
		}).mapToPair(new PairFunction<Tuple2<Long,Tuple3<Integer,Double,Integer>>, Integer, Integer>() {

			@Override
			public Tuple2<Integer, Integer> call(
					Tuple2<Long, Tuple3<Integer, Double, Integer>> t)
					throws Exception {
				return new Tuple2<Integer, Integer>(t._2._1(), t._2._3());
			}
			
		});
	    
	    
	    
	    
	    
	    
//	    state.foreachRDD(new Function<JavaPairRDD<Tuple2<Integer,Integer>,Tuple3<Integer,Integer, Double>>, Void>() {
//
//			@Override
//			public Void call(
//					JavaPairRDD<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>> v1)
//					throws Exception {
//				
//				for(Tuple2<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>> each : v1.collect()) {
//					System.out.println("(" + each._1._1 + ", " + each._1._2 + ") -" + each._2._1() + " - " + each._2._2() + " - " + each._2._3());
//				}
//				return null;
//			}
//	    	
//	    	
//		});
	    
	    
	    predictions.foreachRDD(new Function<JavaPairRDD<Integer,Integer>, Void>() {

			@Override
			public Void call(JavaPairRDD<Integer, Integer> v1) throws Exception {
				
				int correct_num = 0;
				
				for(Tuple2<Integer, Integer> each : v1.collect()) {
					//System.out.println(each._1 + " - " + each._2);
					if(each._1 == each._2) correct_num++;

				}

				System.out.println("Accuracy: " + correct_num / (double) v1.collect().size() * 100);
				
				return null;
				
			}
	    	
	    	
		});
	    
//	    test_stream.mapToPair(new PairFunction<Tuple3<Long,String,String>, Integer, Integer>() {
//
//			@Override
//			public Tuple2<Integer, Integer> call(Tuple3<Long, String, String> t)
//					throws Exception {
//				// TODO Auto-generated method stub
//				return null;
//			}
//	    	
//		});
//	    
//	  
//	    
//	    
//	    JavaPairDStream<Integer, Long> hashed_keys = test_stream.flatMapToPair(new PairFlatMapFunction<Tuple3<Long,String,String>, Integer, Long> () {
//	    	
//	    	@Override
//	    	public java.lang.Iterable<scala.Tuple2<Integer,Long>> call(scala.Tuple3<Long,String,String> t) throws Exception {
//	    		
//	    		String[] terms = t._3().toLowerCase().trim().replaceAll("[^0-9A-Za-z]", " ").split("\\s+");
//				List<Tuple2<Integer, Long>> out = new ArrayList<Tuple2<Integer, Long>>();
//	    		
//				for(int ctr = 0; ctr < terms.length; ctr++) {
//					Integer term_hash = terms[ctr].hashCode();
//					out.add(new Tuple2<Integer, Long>(term_hash, t._1()));
//				}
//	    		
//				return out;
//	    	};
//		});
	    
	    

			
	    
	    
//	    JavaDStream<Tuple2<Integer, Integer>> predcitions = test_stream.map(new Function<Tuple3<Long,String,String>, Tuple2<Integer, Integer>>() {
//
//			@Override
//			public Tuple2<Integer, Integer> call(Tuple3<Long, String, String> t)
//					throws Exception {
//				
//				System.out.println(word_counts.value().values().size());
//				
//				
//				
//				Integer label = Label.label_map.get(t._2());
//				
//				String[] terms = t._3().toLowerCase().trim().replaceAll("[^0-9A-Za-z]", " ").split("\\s+");
//				
//				
//				double maxProbability = Double.NEGATIVE_INFINITY;
//			       
//				   Integer prediction = null;
//			       Set<Integer> candidates = word_sums.value().keySet();
//			       int k = 1;
//			       
//			       for(Integer p : candidates) {
//			    	   double curProbability = 0.0;
//			    	   Map<Tuple2<Integer, Integer>, Integer> termMap = word_counts.value();
//			    	   
//			    	   for(int ctr = 0; ctr < terms.length; ctr++) {
//			    		   Integer temp = termMap.get(new Tuple2<Integer, Integer>(p, terms[ctr].hashCode()));
//			    		   if(temp == null) temp = 0;
//			    		   
//			    		   curProbability += Math.log((temp + k) / (double) (word_sums.value().get(p) + k));
//			    		   //System.out.println(termMap.get(terms[ctr]) + "-" + wordSums.get(p));
//			    	   }
//			    	   
//			    	   
//			    	   if(curProbability > maxProbability) {
//			    		   maxProbability = curProbability;
//			    		   prediction = p;
//			    	   }
//			    	   
//			       }
//			       
//			       return new Tuple2<Integer, Integer>(prediction, label);
//			}
//	    	
//		});
//	

//			@Override
//			public Tuple2<Integer, Integer> call(Tuple3<Long, String, String> t)
//					throws Exception {
//				
//				
//				String label = t._2();
//				
//				String[] terms = t._3().toLowerCase().trim().replaceAll("[^0-9A-Za-z]", " ").split("\\s+");
//				
//				
//				for(int ctr = 0; ctr < terms.length; ctr++) {
//					Integer term_hash = terms[ctr].hashCode();
//					
//					
//					
//				}
//				
//				
//				
//			}
//	    	
//		});
	    
	  
	    total.print();
	    //predictions.print();
	    //state.print();
	    //test_stream_unfolded.print();
	    //partial_scores.print();
	    //conditionals.print();
	    //sums.print();
	    ssc.start();
	    ssc.awaitTermination();
	    
	}
	
}
