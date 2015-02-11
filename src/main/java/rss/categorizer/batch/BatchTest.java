

package rss.categorizer.batch;


import junit.framework.TestCase;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.junit.Test;

import java.util.List;

import static rss.categorizer.batch.BatchClassifier.createLabeledPoints;
import static rss.categorizer.batch.BatchClassifier.createOneLabeledPoint;

public class BatchTest extends TestCase {

  @Test
  public void testOnePointPrediction() throws Exception{

    BatchClassifier batchModel = new BatchClassifier();
    List<LabeledPoint> Traininglist = createLabeledPoints(batchModel.data,1422635169000L, 1422751686000L, batchModel.dic);
    System.out.println(Traininglist.size());

    JavaRDD<LabeledPoint> TraingingSet = batchModel.sc.parallelize(Traininglist.subList(0,300));
    final NaiveBayesModel model = NaiveBayes.train(TraingingSet.rdd(), 1.0);

    double realLabel = TraingingSet.first().label();
    double predictedLabel = model.predict(TraingingSet.first().features());

    assertEquals(realLabel,predictedLabel);
    assertFalse(predictedLabel == realLabel - 1.0);

    LabeledPoint point2 = createOneLabeledPoint(1.0, "technology android video", batchModel.dic);
    double predictedLabelPoint2 = model.predict(point2.features());
    assertFalse(predictedLabelPoint2 == 1.0) ;
    assertEquals(predictedLabelPoint2, 3.0);

    batchModel.sc.close();
  }
}
