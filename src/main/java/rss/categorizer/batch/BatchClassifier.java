package rss.categorizer.batch;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;
import scala.Tuple1;
import scala.Tuple2;
import scala.Tuple3;

import rss.categorizer.model.Label;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by minza on 2/1/15.
 */
public class BatchClassifier {

    SparkConf conf;
    JavaSparkContext sc;
    JavaRDD<String> data;
    BatchDictionary dic;

    public BatchClassifier(){
        this.conf = new SparkConf().setAppName("BatchModel");

        conf.set("spark.executor.extraJavaOptions", " -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+AggressiveOpts -XX:FreqInlineSize=300 -XX:MaxInlineSize=300 ");
        conf.setMaster("local");
        this.sc =  new JavaSparkContext(conf);

        this.data = sc.textFile("./src/main/resources/refined/part-00000");
        this.dic = new BatchDictionary();

    }

    public static void main(String[] arg){

        BatchClassifier batchModel = new BatchClassifier();


        // creates Trainingset and adds words of Trainingset to Dictionary within the given period
        // TODO rewrite times/periods, now timestamp
        List<LabeledPoint> Traininglist = createLabeledPoints(batchModel.data,1422635169000L, 1422751686000L, batchModel.dic);
        JavaRDD<LabeledPoint> TraingingSet = batchModel.sc.parallelize(Traininglist);
        System.out.println(TraingingSet.first());
       // final NaiveBayesModel model = NaiveBayes.train(TraingingSet.rdd(), 1.0);

        batchModel.sc.close();




    }

    /*
    parameters:
        JavaRDD<String> input: RDD of lines from text file
        start and end date of a period the training should be from
        dictionary

     */
    private static List<LabeledPoint> createLabeledPoints(JavaRDD<String> input, final Long startDate, final Long endDate, BatchDictionary dic) {
        if(startDate > endDate){
            System.out.println("wrong date parameters");
            return null;
        }

        JavaRDD<Tuple3<Long, String, String>> dataTuples = input.map(new Function<String, Tuple3<Long, String, String>>() {
            @Override
            public Tuple3<Long, String, String> call(String s) throws Exception {
                String[] tokens = s.replaceFirst("\\(", "").split(",");
                Long timestamp = Long.parseLong(tokens[0]);
                String text = tokens[1].toLowerCase().replaceAll("[^\\s\\dA-Za-z]", "");
                String category = tokens[2].replace(")", "");

                return new Tuple3<Long,String, String>(timestamp, text, category);
            }
        });

        JavaRDD<Tuple2<String, String>> selectedSet = dataTuples.filter(new Function<Tuple3<Long, String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple3<Long, String, String> tuple) throws Exception {
                return (startDate >= tuple._1() && tuple._1() <= endDate);
            }
        }).map(new Function<Tuple3< Long, String, String>, Tuple2< String, String>>() {
            @Override
            public Tuple2<String, String> call(Tuple3<Long, String, String> tupleToTwo) throws Exception {
                return new Tuple2<String, String>(tupleToTwo._2(), tupleToTwo._3());
            }

        });

        // Dictionary
        List<String> potentialDicEntries = selectedSet.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple) throws Exception {
                return tuple._1();
            }
        }).collect();



        for (String terms: potentialDicEntries){
            dic.update(terms);
        }


        // labeled Points

        List<Double> labels = selectedSet.map(new Function<Tuple2<String,String>, Double>() {
            @Override
            public Double call(Tuple2<String, String> tupel) throws Exception {
                Label label = new Label(tupel._2());
                return label.getNumericalValue();
            }
        }).collect();

        List<String> points = selectedSet.map(new Function<Tuple2<String,String>, String>() {
            @Override
            public String call(Tuple2<String, String> tupel) throws Exception {

                return tupel._1();
            }
        }).collect();



        // labeled Points
        double[] labelsArray = ArrayUtils.toPrimitive(labels.toArray(new Double[labels.size()]));

        return labelPoints(labelsArray,points,dic);
    }

    private static List<LabeledPoint> labelPoints(double[] labels, List<String> points, BatchDictionary dic){
        if(labels.length != points.size()){
            System.out.println("size of labels and points don't match");
        }

        List<LabeledPoint> pointList = new ArrayList<LabeledPoint>();
        double currentLabel;
        for(String point : points){
            // label at position i
            int i = 0;
            int index;
            currentLabel = labels[i];

            double[] featureVector = new double[dic.getSize()];
            String[] words = point.split("\\s+");

            for(String w : words){
                w.toLowerCase().replaceAll("[^\\dA-Za-z]", "").trim();

                if(!w.isEmpty()) {
                    // index in dictionary for w
                    if(dic.getDictionary().containsKey(w)){
                        index = dic.getDictionary().get(w).get(0);
                        featureVector[index] = dic.getTfIdfVector()[index];

                    }
                    else{
                        System.out.println(w + ": not found");
                    }

                }
            }


            LabeledPoint lP = new LabeledPoint(currentLabel, Vectors.sparse(dic.getSize(), dic.getIndexVector(), featureVector));
            pointList.add(lP);
            i++;
        }
        return  pointList;
    }

}
