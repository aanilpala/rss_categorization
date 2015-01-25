package feed.util;

import java.io.Serializable;
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

public class FeedRefiner implements Serializable{  //Sorts the feed according to timestamp

	
	private SparkConf sparkConf;
	private JavaSparkContext ctx;
	private List<FeedItem> sortedFeedItems;
	private JavaRDD<String> lines;

	public FeedRefiner(String inputPath) {
		
		sparkConf = new SparkConf().setAppName("FeedSorter").setMaster("local");
		
		sparkConf.set("spark.hadoop.validateOutputSpecs", "false");
		
	    ctx = new JavaSparkContext(sparkConf);	
		lines = ctx.textFile(inputPath);

		JavaRDD<String> _lines = lines;
		
		JavaRDD<FeedItem> feedItems = _lines.flatMap(new FlatMapFunction <String, FeedItem>() {
			
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
		
		sortedFeedItems = feedItems.distinct().sortBy(new Function<FeedItem, Long>() {

			DateFormat format = new SimpleDateFormat("dd MMM yyyy HH:mm:ss zzz", Locale.ENGLISH);
			
			@Override
			public Long call(FeedItem item) throws Exception {
				return format.parse(item.getPubDate()).getTime();
			}
			
			
		}, true, 1).collect();
		
		ctx.close();
	}
	
	public List<FeedItem> getRefinedItems() {
		
		return sortedFeedItems;
		
	}

	
	
}
