package feed.util;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple3;


public class FeedRefiner implements Serializable{  //Sorts the feed according to timestamp

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private SparkConf sparkConf;
	private JavaSparkContext ctx;
	private JavaRDD<Tuple3<Long, String, String>> sortedFeedItems;
	private JavaRDD<String> lines;


	public FeedRefiner(String inputPath) {
		
		sparkConf = new SparkConf().setAppName("FeedRefiner").setMaster("local");
		
		sparkConf.set("spark.hadoop.validateOutputSpecs", "false");
		
	    ctx = new JavaSparkContext(sparkConf);	
		lines = ctx.textFile(inputPath);
		
		

		
	}
	
	
	public static void main(String[] args) {
		
		FeedRefiner feedRefiner = new FeedRefiner("./src/main/resources/rss-arch.txt");
		
		feedRefiner.refineFeed();
		
		feedRefiner.ctx.close();
		
		//feedRefiner.dump();
		
		//List<FeedItem> a = feedRefiner.getRefinedItems();
		
		//feedRefiner.close();
		
	}

		
	private void dump() {

		sortedFeedItems.saveAsTextFile("./src/main/resources/refined");
		
	}

	public void refineFeed() {
		
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
				
				if(tuple._2().split(" |,").length < 10) return false;
				else return true;
			}
		});
		
		
		
		sortedFeedItems = filteredLabeledText.distinct().sortBy(new Function<Tuple3<Long,String,String>, Long>() {

			@Override
			public Long call(Tuple3<Long, String, String> tuple) throws Exception {
				return tuple._1();
			}
		}, true, 1);
		
	}
	
	public List<Tuple3<Long, String, String>> getRefinedItems() {
		
		return sortedFeedItems.collect();
		
	}
	
	public void close() {
		ctx.close();
	}
	


	
	
}
