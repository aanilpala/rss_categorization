package feed.util;

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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import feed.model.FeedItem;

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
	
	
	public void castList(List<FeedItem> feedItems) {
		for(FeedItem each : feedItems) {
			out.println(each.toString());
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		
		
		//FeedRefiner feedRefiner = new FeedRefiner("./src/main/resources/rss-arch.txt");
		
		SparkConf sparkConf = new SparkConf().setAppName("FeedSorter").setMaster("local");
		
		sparkConf.set("spark.hadoop.validateOutputSpecs", "false");
		
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);	
		JavaRDD<String> lines = ctx.textFile("./src/main/resources/rss-arch.txt");
		
		JavaRDD<FeedItem> feedItems = lines.flatMap(new FlatMapFunction <String, FeedItem>() {
			
			@Override
			public Iterable<FeedItem> call(String line) {
				
				FeedItem feedItem = null;
				String tokens[] = line.split(",");
				
				if(tokens.length > 5) {
					for(int ctr = 5; ctr < tokens.length-1; ctr++){
						tokens[4] += " " + tokens[ctr]; 
					}
					
					tokens[5] = tokens[tokens.length-1];
					
					if(tokens[4].split(" ").length < 10) return Arrays.asList();
					
					
					feedItem = new FeedItem(tokens[1].trim(), tokens[2].trim().replace(",", " "), tokens[3].trim().replace(",", " "), tokens[4].trim().replace(",", " "), tokens[5].trim().replace(",", " "));
				}
				else {
					if(tokens[3].split(" ").length < 10) return Arrays.asList();
					feedItem = new FeedItem(tokens[1].trim().replace(",", " "), tokens[2].trim().replace(",", " "), tokens[3].trim().replace(",", " "), " ", tokens[4].trim().replace(",", " "));
				}
					
				
				
				return Arrays.asList(feedItem);
			}
			
			
		});
		
		List<FeedItem> sortedFeedItems = feedItems.distinct().sortBy(new Function<FeedItem, Long>() {

			DateFormat format = new SimpleDateFormat("dd MMM yyyy HH:mm:ss zzz", Locale.ENGLISH);
			
			@Override
			public Long call(FeedItem item) throws Exception {
				return format.parse(item.getPubDate()).getTime();
			}
			
			
		}, true, 1).collect();
		
		ctx.close();
				
		//List<FeedItem> refinedItems = feedRefiner.getRefinedItems();
		
		FeedCaster feedCaster = new FeedCaster("localhost", 9999);
		
		feedCaster.castList(sortedFeedItems);
		
		
	}
}
