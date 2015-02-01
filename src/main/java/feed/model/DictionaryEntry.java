package feed.model;

import java.io.Serializable;

public class DictionaryEntry implements Serializable {
	
	private Integer index; // hash-value will go here
	private Integer df; // document frequency
	private String term;
	
	public DictionaryEntry(String term) {
		this.term = term;
		this.df = 1;
		this.index = term.hashCode();
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
		return "(" + index + ", " + term + ", " + df + ")"; 
	}

	public void incrementDf() {
		df++;
	}
	
	
	
}
