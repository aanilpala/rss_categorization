package rss.categorizer.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.map.HashedMap;

public class DictionaryEntry implements Serializable {
	
	private Integer index; // hash-value will go here
	private Integer df; // document frequency
	private String term;
	private Map<Integer, Integer> batch2df_map;
	private Integer init_batch; // the first batch the term showed up
	
	public DictionaryEntry(String term, Integer batch_num) {
		this.term = term;
		this.df = 1;
		this.index = term.hashCode();
		this.init_batch = batch_num;
		this.batch2df_map = new HashMap<Integer, Integer>();
		this.batch2df_map.put(batch_num, 1);
	}

	public Integer getIndex() {
		return index;
	}

	public String getTerm() {
		return term;
	}
	
	@Override
	public String toString() {
		
		String map_print = "b2df mapping: ";
		
		Set<Integer> keys = batch2df_map.keySet();
		
		for(Integer key : keys) {
			map_print += key + ":" + batch2df_map.get(key) + ", ";
		}
		
		return term + ", " + map_print;
	}

	public void incrementDf() {
		df++;
	}

	public void incrementDfBy(int size) {
		df += size;
		
	}
	
	public Integer getDfByBatch(int batch_num) {
		return batch2df_map.get(batch_num);
	}
	
	public Integer getTotalDf() {
		int total_df = 0;
		
		Set<Integer> keys = batch2df_map.keySet();
		
		for(Integer key : keys) {
			total_df += batch2df_map.get(key);
		}
		
		return total_df;
	}
	
	public void updateBatch(int batch_num, int df) {
		Integer cur_num = batch2df_map.get(batch_num);
		if(cur_num == null) batch2df_map.put(batch_num, df);
		else {
			batch2df_map.put(batch_num, cur_num + df);
		}
	}
	
	public Integer getBatchNumber() {
		return init_batch;
	}
	
	
	
}
