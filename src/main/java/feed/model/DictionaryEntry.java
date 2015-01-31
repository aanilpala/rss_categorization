package feed.model;

public class DictionaryEntry {
	
	private Integer index; // hash-value will go here
	private Integer df; // document frequency
	private String term;
	
	public DictionaryEntry(String term) {
		this.term = term;
		this.df = 1;
		this.index = term.hashCode();
	}
	
	public void incrementDF() {
		this.df++;
	}

	public Integer getIndex() {
		return index;
	}

	public Integer getDf() {
		return df;
	}

	public String getTerm() {
		return term;
	}
	
	
	
}
