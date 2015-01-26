package sandbox;

import feed.model.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

import java.util.*;

/**
 * Created by minza on 1/25/15.
 */
public class BayesClassifier {

    List<LabeledPoint> pointsLabeled = new ArrayList<LabeledPoint>();




    public static void main(String[] arg){
        HashMap<String, Double> labels = new HashMap<String, Double>();
        labels.put("sport",0.0);
        labels.put("politic",1.0);
        labels.put("entertainment",2.0);
        HashMap<String,Integer> dictionary = new HashMap<String,Integer>();
        List<LabeledPoint> pointList = new ArrayList<LabeledPoint>();



        FeedItem[] inputTraining = {new FeedItem("somewhen","guid","fun with sports","sport is great","sport"),new FeedItem("somewhen","guid","health stuff","sport should be done","sport"), new FeedItem("somewhen","guid","yoga","yoga is serious sport","sport"), new FeedItem("somewhen","guid","White House","the white house in Washington","politic"), new FeedItem("somewhen","guid","Big Politics","All members of the Federation","politic"), new FeedItem("somewhen","guid","The Global View","Moving to a new area with global politics","politic")};
        int index = 0;

        //trainingset
        for(FeedItem f : inputTraining){
            String label = f.getCategory();
            String content = f.getTitle() + " " + f.getDescription();

            // we need a proper dictionary
            String[] words = content.split("\\s+");

            double[] vector = new double[0];
            for(String w : words){
                vector = new double[100];
                // Preprocessing stuff need to be done at some point, maybe also use stop list
               // w.replace(",","");
                if(dictionary.containsKey(w)) {
                    vector[dictionary.get(w)] += 1.0;
                    }
                else
                dictionary.put(w, index);
                vector[dictionary.get(w)] = 1.0;
                index++;

            }
            // we should use sparse
            LabeledPoint lp = new LabeledPoint(labels.get(label), Vectors.dense(vector));
            pointList.add(lp);

        }

        SparkConf conf = new SparkConf().setAppName("test");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<LabeledPoint> trainingPoints = sc.parallelize( pointList );
        final NaiveBayesModel model = NaiveBayes.train(trainingPoints.rdd(), 1.0);

        // testpoints = trainingpoints
        JavaRDD<LabeledPoint> testPoints = trainingPoints;
        JavaPairRDD<Double, Double> predictionAndLabel =
                testPoints.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    @Override public Tuple2<Double, Double> call(LabeledPoint p) {
                        return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
                    }
                });
        double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
            @Override public Boolean call(Tuple2<Double, Double> pl) {
                return pl._1().equals(pl._2());
            }
        }).count() / (double) testPoints.count() * 100;

        System.out.println(accuracy);

    }



}
