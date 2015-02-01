package rss.categorizer.util.feed;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import rss.categorizer.config.TimeConversion;
import rss.categorizer.model.FeedItem;
import scala.Tuple2;
import scala.Tuple3;

public class FeedCaster {

	private String hostName;
	private int portNumber;
	private ServerSocket clientSocket;
	private PrintWriter out;
	
	public FeedCaster(String hostName, int portNumber) {
		this.hostName = hostName;
		this.portNumber = portNumber;
		
		try {
			ServerSocket serverSocket = new ServerSocket(portNumber);
			Socket clientSocket = serverSocket.accept();
			out = new PrintWriter(clientSocket.getOutputStream(), true);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public void castList(List<Tuple3<Long, String, String>> items) {
		Long lastStreamed = 0L, waitingTime;
		for(Tuple3<Long, String, String> each : items) {
			if(lastStreamed != 0) waitingTime = (each._1() - lastStreamed) / TimeConversion.flow_rate;
			else waitingTime = 10L;
			try {
				Thread.sleep(waitingTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			out.println(each.toString());
			System.out.println("Item casted with " + waitingTime + "ms delay: " + each.toString());
			lastStreamed = each._1();
			
		}
	}
	
	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("FeedRefiner").setMaster("local");
		
		// disabling default verbouse mode of the loggers
		Logger.getLogger("org").setLevel(Level.FATAL);
		Logger.getLogger("akka").setLevel(Level.FATAL);
		
		sparkConf.set("spark.hadoop.validateOutputSpecs", "false");
		
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);	
		JavaRDD<String>  lines = ctx.textFile("./src/main/resources/rss-arch.txt");
		
		JavaRDD<Tuple3<Long, String, String>> labeledText = lines.map(new Function<String, Tuple3<Long, String, String>>() {

			@Override
			public Tuple3<Long, String, String> call(String line)
					throws Exception {
				String tokens[] = line.split(",");
				String text, category;
				
				if(tokens.length > 5) {
					
					text = tokens[3];
					
					for(int ctr = 5; ctr < tokens.length-1; ctr++){
						text += " " + tokens[ctr]; 
					}
					
					category = tokens[tokens.length-1];
					
				}
				else {
					
					text = tokens[3];
					category = tokens[4];
				}
					
				DateFormat format = new SimpleDateFormat("dd MMM yyyy HH:mm:ss zzz", Locale.ENGLISH);
				
				return new Tuple3<Long, String, String>(format.parse(tokens[1]).getTime(), text.replace(",", " "), category.trim());
			}
		});
		
		
		
		JavaRDD<Tuple3<Long, String, String>> filteredLabeledText = labeledText.filter(new Function<Tuple3<Long,String,String>, Boolean>() {

			@Override
			public Boolean call(Tuple3<Long, String, String> tuple) throws Exception {
				
				if(tuple._2().split(" |,").length < 5) return false; // Reject Feed Items with too less words
				if(tuple._1() < 1421236800000L) return false;  // Reject Feed Items published before the 14th January, 12:00 GMT -data is too sparse prior to this point in time.
				else return true;
			}
		});
		
		
		JavaPairRDD<Integer, Tuple3<Long, String, String>> keyedFilteredLabeledText = filteredLabeledText.keyBy(new Function<Tuple3<Long,String,String>, Integer>() {
			
			@Override
			public Integer call(Tuple3<Long, String, String> tuple)
					throws Exception {
				return tuple._2().hashCode();
			}
		});
		
		JavaPairRDD<Integer, Tuple3<Long, String, String>> distinctKeyedFilteredLabeled = keyedFilteredLabeledText.foldByKey(null, new Function2<Tuple3<Long,String,String>, Tuple3<Long,String,String>, Tuple3<Long,String,String>>() {
			
			@Override
			public Tuple3<Long, String, String> call(Tuple3<Long, String, String> t1,
					Tuple3<Long, String, String> t2) throws Exception {
				return t2;
			}
		});
		
		JavaRDD<Tuple3<Long, String, String>> distinctFilteredLabeledText = distinctKeyedFilteredLabeled.map(new Function<Tuple2<Integer,Tuple3<Long,String,String>>, Tuple3<Long,String,String>>() {

			@Override
			public Tuple3<Long, String, String> call(Tuple2<Integer, Tuple3<Long, String, String>> tuple)
					throws Exception {
				return tuple._2;
			}
		});
		
		
		JavaRDD<Tuple3<Long, String, String>> sortedFeedItems = distinctFilteredLabeledText.sortBy(new Function<Tuple3<Long,String,String>, Long>() {

			@Override
			public Long call(Tuple3<Long, String, String> tuple) throws Exception {
				return tuple._1();
			}
		}, true, 1);
		
		
		sortedFeedItems.saveAsTextFile("./src/main/resources/refined");
		
		FeedCaster feedCaster = new FeedCaster("localhost", 9999);
		
		feedCaster.castList(sortedFeedItems.collect());
		
		ctx.close();
		
		
	}
}
