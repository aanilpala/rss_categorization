package feed.reader;

import feed.model.Feed;
import feed.model.FeedMessage;


public class ReadTest {
  public static void main(String[] args) {
    RSSFeedParser parser = new RSSFeedParser("http://rss.cnn.com/rss/edition_entertainment.rss");
    Feed feed = parser.readFeed();
    System.out.println(feed.getPubDate());
    for (FeedMessage message : feed.getMessages()) {
      System.out.println(message);

    }

  }
} 
