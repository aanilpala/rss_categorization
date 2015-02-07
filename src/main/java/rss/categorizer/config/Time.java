package rss.categorizer.config;

public class Time {

	public static int scaling_factor = 50000; // This means in manual streaming we stream 1000 times faster than the real data-rate of the RSS Feeds.
	
	public static long an_hour = 60*60*1000 / scaling_factor;
	public static long a_day = 24*60*60*1000 / scaling_factor;
	
	public static long start_point = 1421280000000L;
	
	//public static long convertFromRealTime
	
	
}
