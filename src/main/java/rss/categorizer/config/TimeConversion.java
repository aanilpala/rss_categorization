package rss.categorizer.config;

public class TimeConversion {

	public static int flow_rate = 1000; // This means in manual streaming we stream 1000 times faster than the real data-rate of the RSS Feeds.
	
	public static long an_hour = 60*60*1000 / flow_rate;
	public static long a_day = 24*60*60*1000 / flow_rate;
	
	//public static long convertFromRealTime
	
	
}
