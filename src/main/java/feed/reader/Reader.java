package feed.reader;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import feed.model.Feed;
import feed.model.FeedMessage;

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
	    this.feed = parser.readFeed();
	}
	
	public void sinkItems(FileWriter fw) {
		
		
		long max = Long.MIN_VALUE;
		for (FeedMessage message : feed.getMessages()) {
			
			DateFormat format = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss zzz", Locale.ENGLISH);
			
			try {
				long pubdate = format.parse(message.getPubDate()).getTime();
				if (pubdate > lastUpdate) {
					fw.write(message.toString() + topic + "\n");
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
