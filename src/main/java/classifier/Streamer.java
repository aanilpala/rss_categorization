package classifier;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import scala.Tuple2;
import feed.model.DictionaryEntry;


public class Streamer {

	public static Long window_duration = 1000L*60; // this corresponds to an hour
	public static Long sliding_interval= 1000L*60; // this corresponds to an hour
	public static Long batch_interval = 1000L*60; // this corresponds to 2 hours
	
	public static Long training_interval = 3000L*60;
	
	public static void main(String[] args) {
		
	
		
		SparkConf conf = new SparkConf().setAppName("streamer-playaround").setMaster("local[2]");
			
		
		// disabling default verbouse mode of the loggers
		Logger.getLogger("org").setLevel(Level.FATAL);
	    Logger.getLogger("akka").setLevel(Level.FATAL); 
	   
		
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(batch_interval));  //batch interval discretizes the continous input
		
	    ssc.checkpoint("./src/main/resources/checkpoint");
	    	    
	    JavaReceiverInputDStream<String> stream = ssc.socketTextStream("localhost", 9999);

	    
	    
	
	    
	    // String to Tuple3 Conversion
	    JavaDStream<Tuple2<String, String>> tuple_stream = stream.map(new Function<String, Tuple2<String, String>>() {

			@Override
			public Tuple2<String, String> call(String item) throws Exception {
				
				String[] tokens = item.split(",");
				return new Tuple2<String, String>(tokens[2], tokens[1]);
				
			}
		});
	    
//	    JavaRDD<Tuple2<String, String>> a = tuple_stream.compute(new Time(System.currentTimeMillis() + training_interval));
//	    
//	    a.saveAsTextFile("./training");
	    
	    
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
	
	Function2<Integer, Integer, Integer> windowReducer = new Function2<Integer, Integer, Integer>() {
		  @Override public Integer call(Integer i1, Integer i2) throws Exception {
		    return i1 + i2;
		  }
		};
	
}
