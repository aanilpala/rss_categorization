package stream;


import org.apache.spark.SparkConf;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;




public class Main {

	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("streamer-playaround").setMaster("local[2]");
		
	    JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(5000));

	    ssc.checkpoint("./src/main/resources/checkpoint");
	    	    
	    JavaReceiverInputDStream<String> stream = ssc.socketTextStream("localhost", 9999);

	    
	    //JavaPairInputDStream<Long, String> aa = ssc.fileStream("./src/main/resources/refined");	    
	   
	    
	 
	    stream.print();
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
