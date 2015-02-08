package rss.categorizer.stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import breeze.linalg.scaleAdd;
import rss.categorizer.config.Time;
import scala.Option;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Iterator;
import scala.collection.mutable.HashMap;

public class SimpleStreamer {
	private static Long batch_interval = 6*Time.an_hour_scaled;
	
	private static Long training_dur = 6*Time.an_hour_scaled;
	private static Long training_interval = 24*Time.an_hour_scaled;
	
	private static boolean incremental_update = true;

	//private static NaiveBayesianMiner nb;
	
	private static List<LabeledPoint> labeledpoints;
	
public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("streamer").setMaster("local[2]");
		
		Logger.getLogger("org").setLevel(Level.FATAL);
		Logger.getLogger("akka").setLevel(Level.FATAL);
	    
	    conf.set("spark.driver.allowMultipleContexts", "true");
	    
	    
	    final JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(batch_interval));  //batch interval discretizes the continous input
	    ssc.checkpoint("/tmp/spark/checkpoint");
	    JavaReceiverInputDStream<String> stream = ssc.socketTextStream("localhost", 9999);
	    
		
		labeledpoints = new ArrayList<LabeledPoint>();
		
	
		
	    // String to Tuple3 Conversion
	    JavaDStream<Tuple3<Long, String, String>> tuple_stream = stream.map(new Function<String, Tuple3<Long, String, String>>() {

			@Override
			public Tuple3<Long, String, String> call(String item) throws Exception {
				
				String[] tokens = item.replaceAll("\\(|\\)", "").split(",");
				return new Tuple3<Long, String, String>(Long.parseLong(tokens[0]), tokens[2], tokens[1]);
				
			}
		});
	    
	    // Tuple3 to LabeledPoints Conversion
	    JavaDStream<Tuple3<Long, String, LabeledPoint>> labeled_points = tuple_stream.map(new Function<Tuple3<Long,String,String>, Tuple3<Long, String, LabeledPoint>>() {

			@Override
			public Tuple3<Long, String, LabeledPoint> call(
					Tuple3<Long, String, String> t) throws Exception {
				
				String[] terms = t._3().toLowerCase().trim().replaceAll("[^0-9A-Za-z]", " ").split("\\s+");
				
				HashMap<Integer, Integer> term_map = new HashMap<Integer, Integer>();
				
				for(int ctr = 0; ctr < terms.length; ctr++) {
					Integer hash = terms[ctr].hashCode();
					
					Option<Integer> freq = term_map.get(hash);
					if(freq.isEmpty()) term_map.put(hash, 1);
					else term_map.put(hash, freq.get()+1);
				}
				
				Iterator<Tuple2<Integer, Integer>> it = term_map.iterator();
				int indices[] = new int[terms.length];
				double values[] = new double[terms.length];
			
				int ctr = 0;
				while(it.hasNext()) {
					Tuple2<Integer, Integer> cur = it.next();
					indices[ctr] = cur._1;
					values[ctr++] = cur._2;
				}
					 
				Arrays.sort(indices);
				Arrays.sort(values);
				
				SparseVector features = new SparseVector(1000, indices, values);
				LabeledPoint labeled_point = new LabeledPoint((double) Label.label_map.get(t._2()), features);
				
				if((t._1()/Time.scaling_factor % training_interval) > training_dur) {
					NaiveBayes.train(ssc.sc().parallelize(labeledpoints).rdd());
					System.out.println("A GOOD SIGN");
				}
				else {
					labeledpoints.add(labeled_point);
				}
				
				return new Tuple3<Long, String, LabeledPoint>(t._1(), t._2(), labeled_point);
				
			}
	    });
	    

	    labeled_points.print();
	    ssc.start();
	    ssc.awaitTermination();
	    
	    
	}
};
