package rss.categorizer.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;

public class DictionaryEntry implements Serializable {
	
	private Integer index; // hash-value will go here
	private Integer df; // document frequency
	private String term;
	private Map<Integer, Integer> batch2df_map;
	private Integer batch_num; // relevant only before put into the dictionary
	
	public DictionaryEntry(String term, Integer batch_num) {
		this.term = term;
		this.df = 1;
		this.index = term.hashCode();
		this.batch_num = batch_num;
		this.batch2df_map = new HashMap<Integer, Integer>();
		this.batch2df_map.put(batch_num, 1);
	}

	public Integer getIndex() {
		return index;
	}

	public void setDf(Integer df) {
		this.df = df;
	}

	public Integer getDf() {
		return df;
	}

	public String getTerm() {
		return term;
	}
	
	@Override
	public String toString() {
		return term + ", " + df;
	}

	public void incrementDf() {
		df++;
	}

	public void incrementDfBy(int size) {
		df += size;
		
	}
	
	public Integer queryBatch(int batch_num) {
		return batch2df_map.get(batch_num);
	}
	
	public void updateBatch(int batch_num, int df) {
		Integer cur_num = batch2df_map.get(batch_num);
		if(cur_num == null) batch2df_map.put(batch_num, df);
		else {
			batch2df_map.put(batch_num, cur_num + df);
		}
	}
	
	public Integer getBatchNumber() {
		return batch_num;
	}
	
	
	
}
