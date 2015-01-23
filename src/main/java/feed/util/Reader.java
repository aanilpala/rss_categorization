package feed.util;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import feed.model.Feed;
import feed.model.FeedItem;

public class Reader {

	private String feedUrl;
	private String topic;
	private Feed feed;
	private long lastUpdate;

	public Reader(String feedUrl, String topic, long lastUpdate) {
		this.feedUrl = feedUrl;
		this.topic = topic;
		this.lastUpdate = lastUpdate;
		RSSFeedParser parser = new RSSFeedParser(feedUrl);
	    this.feed = parser.readFeed(topic);
	}
	
	public void sinkItems(FileWriter fw) {
		
		
		long max = Long.MIN_VALUE;
		for (FeedItem message : feed.getMessages()) {
			
			DateFormat format = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss zzz", Locale.ENGLISH);
			
			try {
				long pubdate = format.parse(message.getPubDate()).getTime();
				if (pubdate > lastUpdate) {
					fw.write(message.toString() + "\n");
					if(pubdate > max) {
						max = pubdate;
					}
				}
					    
			} catch (ParseException | IOException e) {
				e.printStackTrace();
			}
		 
		}
		
		if(max != Long.MIN_VALUE)
			this.lastUpdate = max;
		
	}
	
	@Override
	public String toString() {
		return this.feedUrl + " " + this.topic + " " + this.lastUpdate + "\n";
	}
	
	public long getLastUpdate() {
		return lastUpdate;
	}


}
