package feed.model;

import java.io.Serializable;

/*
 * Represents one RSS message
 */
public class FeedItem implements Serializable, Comparable<FeedItem>{

  String title;
  String description;
  String link;
  String author;
  String guid;
  String pubDate;
  String category;

  public String getCategory() {
	return category;
}

public void setCategory(String category) {
	this.category = category;
}

public FeedItem(String pubDate, String guid, String title, String description, String category) {
	  
	  this.pubDate = pubDate;
	  this.guid = guid;
	  this.title = title;
	  this.description = description;
	  this.category = category;
	  
  }

  public FeedItem() {
  }

  public String getPubDate() {
	return pubDate;
  }

  public void setPubDate(String pubDate) {
	this.pubDate = pubDate;
}

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getLink() {
    return link;
  }

  public void setLink(String link) {
    this.link = link;
  }

  public String getAuthor() {
    return author;
  }

  public void setAuthor(String author) {
    this.author = author;
  }

  public String getGuid() {
    return guid;
  }

  public void setGuid(String guid) {
    this.guid = guid;
  }

  @Override
  public String toString() {
    return pubDate + ", " + title + ", " + description + ", " + category;
  }
  
  @Override
  public int compareTo(FeedItem o) {
	
	return this.title.trim().equals(o.title) == true ? 0 : 1;
  } 
}