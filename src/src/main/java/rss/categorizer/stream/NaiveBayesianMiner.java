package rss.categorizer.stream;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.regression.LabeledPoint;


public class NaiveBayesianMiner implements Serializable  {

	private Integer state;
	private NaiveBayesModel naive_bayes_model;
	private Set<Integer> dictionary_indices;
	public List<LabeledPoint> training_set;
	private boolean incremental;
	

	public NaiveBayesianMiner(boolean incremental) {
		this.state = 1;
		this.dictionary_indices = new TreeSet<Integer>();
		this.training_set = new ArrayList<LabeledPoint>();
		this.incremental = incremental;
	}
	
	public int getCurState() {
		return state;
	}

	public void setCurState(int state) {
		this.state = state;
	}

	public void insert_into_dictionary_index(Integer index) {
		dictionary_indices.add(index);
		System.out.println(index);
	}
	
	public void insert_into_training_set(LabeledPoint labeled_point) {
		training_set.add(labeled_point);
		System.out.println(training_set.size());
	}
	
	
	public void resetTrainingSession() {
		if(!incremental) {
			//this.training_set.clear();
			//this.dictionary_indices.clear();
		}
		else; // do nothing;
	}

	public int getDictionarySize() {
		return dictionary_indices.size();
	}

	public double predict(SparseVector features) {
		return this.naive_bayes_model.predict(features);
		//return 1.0;
	}

	public void prepareModel(JavaSparkContext sc) {
		naive_bayes_model = NaiveBayes.train(sc.parallelize(this.training_set).rdd());
	}


};