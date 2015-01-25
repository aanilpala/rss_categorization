package sandbox;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by minza on 1/24/15.
 */
public class tutorial {
    public static void main(String[] arg) throws Exception{
        String data = "/home/minza/Desktop/AIM3/Project/AIM3_Project/src/main/resources/playData.txt";
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);


       // JavaRDD<String> distData = sc.parallelize(data);
        JavaRDD<String> lines = sc.textFile("/home/minza/Desktop/AIM3/Project/AIM3_Project/src/main/resources/playData.txt");
        JavaRDD<String> words = lines.flatMap(new myFlatMap());


        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });
        System.out.println(counts.first());







    }

    private static class myFlatMap implements FlatMapFunction<String, String> {
        @Override
        public Iterable<String> call(String s) throws Exception {
            return Arrays.asList(s.split(" "));
        }
    }


}

