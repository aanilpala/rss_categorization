package stream;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


public class Main {

	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("streamer-playaround");
		//JavaSparkContext sc = new JavaSparkContext(conf);
		
	    JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));

		
	    
	    
	}
}
