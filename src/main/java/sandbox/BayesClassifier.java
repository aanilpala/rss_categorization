package sandbox;

import feed.model.*;
import feed.model.Dictionary;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

import java.util.*;


/**
 * Created by minza on 1/25/15.
 */
public class BayesClassifier {

    List<LabeledPoint> pointsLabeled = new ArrayList<LabeledPoint>();



    public static void main(String[] arg) {




        FeedItem[] inputTraining = {new FeedItem("somewhen", "guid", "fun with sports", "sport is great", "sport"), new FeedItem("somewhen", "guid", "health stuff", "sport should be done", "sport"), new FeedItem("somewhen", "guid", "yoga", "yoga is serious sport", "sport"), new FeedItem("somewhen", "guid", "White House", "the white house in Washington", "politic"), new FeedItem("somewhen", "guid", "Big Politics", "All members of the Federation", "politic"), new FeedItem("somewhen", "guid", "The Global View", "Moving to a new area with global politics", "politic")};
        FeedItem[] inputTest = {new FeedItem("somewhen", "guid", "fun with sports", "sport is great", "sport"), new FeedItem("somewhen", "guid", "sports are danger!", "sport causes millions of accidents", "sport"), new FeedItem("somewhen", "guid", "fun with sports", "barack obama has releases a new health care deployment with global influence", "politic"), new FeedItem("somewhen", "guid", "fun with sports", "sport is great", "politic")};


        SparkConf conf = new SparkConf().setAppName("test");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<LabeledPoint> pointList = createLabeledPointSet(inputTraining);
        // trainingpoints
        JavaRDD<LabeledPoint> trainingPoints = sc.parallelize(pointList);
        final NaiveBayesModel model = NaiveBayes.train(trainingPoints.rdd(), 1.0);

        //testpoints
        List<LabeledPoint> testList= createLabeledPointSet(inputTest);
        JavaRDD<LabeledPoint> testPoints = sc.parallelize(testList);

        JavaPairRDD<Double, Double> predictionAndLabel =
                testPoints.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    @Override
                    public Tuple2<Double, Double> call(LabeledPoint p) {
                        return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
                    }
                });
        double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Double, Double> pl) {
                return pl._1().equals(pl._2());
            }
        }).count() / (double) testPoints.count() * 100;

        System.out.println(accuracy);

        // predict one point
        Vector newPoint =createLabeledPointSet(inputTest).get(2).features();
        double predictedLabel = model.predict(newPoint);

        String[] labels = {"sport","politic","entertainment"};

        System.out.println("Predicted Label: " + predictedLabel + " means " + labels[(int)predictedLabel]);

    }


    public static List<LabeledPoint> createLabeledPointSet(FeedItem[] input){
        HashMap<String, Double> labels = new HashMap<String, Double>();
        labels.put("sport", 0.0);
        labels.put("politic", 1.0);
        labels.put("entertainment", 2.0);

        List<LabeledPoint> pointList = new ArrayList<LabeledPoint>();
        Dictionary dictionary = new Dictionary("./src/main/resources/rss-arch.txt");

        for (FeedItem f : input) {
            String label = f.getCategory();
            String content = f.getTitle() + " " + f.getDescription();
            String[] words = content.split("\\s+");
            double[] bow = dictionary.initBagOfWordVector();
            int[] indexVector = dictionary.getIndexVector();

            for (String word : words) {
                word = word.toLowerCase().replaceAll("[^\\dA-Za-z]", "").trim();
                if (dictionary.getDictionary().containsKey(word)) {
                    int pos = dictionary.getDictionary().get(word);
                    bow[pos] += 1.0;
                }else{
                    System.out.println(">" + word +"< is not in Dictionary");
                }
            }
            LabeledPoint lp = new LabeledPoint(labels.get(label), Vectors.sparse(dictionary.getSize(), indexVector, bow));
            pointList.add(lp);

        }


        return pointList;
    }

}
