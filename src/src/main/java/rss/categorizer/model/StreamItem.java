package rss.categorizer.model;

public class StreamItem {
	private String terms;
	private Long timestamp;
	private String label;
	private String prediction;
	
	public StreamItem(String terms, Long timestamp, String label) {
		this.terms = terms;
		this.timestamp = timestamp;
		this.label = label;
		this.prediction = "";
	}
	
	public String getTerms() {
		return terms;
	}
	public Long getTimestamp() {
		return timestamp;
	}
	public String getLabel() {
		return label;
	}
	public void setPrediction(String prediction) {
		this.prediction = prediction;
	}
	public boolean predictedCorrect() {
		return (prediction.equals(label));
	}
	
	
}
