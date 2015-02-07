package rss.categorizer.stream;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Label {

	 public static final Map<String, Integer> label_map;
	    static {
	        Map<String, Integer> map = new HashMap<String, Integer>();
	        map.put("politics", 0);
	        map.put("business", 1);
	        map.put("health", 2);
	        map.put("technology", 3);
	        map.put("entertainment", 4);
	        map.put("sports", 5);
	        
	        label_map = Collections.unmodifiableMap(map);
	    }
	    
	    
	  public static final double[] labels = new double[6];
	  	static {
	  		for(int ctr = 0; ctr < 6; ctr++) {
	  			labels[ctr] = ctr;
	  		}
	  	}
	
}
