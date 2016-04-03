
package experiments;
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._


object WordCount_Checkpointing_ {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark Word Count With Checkpoint"))

    //val sc = new SparkContext(sparkConf)
    sc.addSparkListener(new SparkListener() {
      override def onJobStart(jobStart: SparkListenerJobStart) {
        super.onJobStart(jobStart)
        println("ADAPT: INSIDE Job Start Listener ");
        val props = propertiesAsScalaMap(jobStart.properties)
        if (props.contains("spark.rdd.scope")) {
          if (props.contains("name") &&  props("name") == "checkpoint") {
            println("JobID " + jobStart.jobId);
            println("This is a checkpointing job for RDD - " + props("id"))
            println("StartTime - " + jobStart.time)
          }
        }
      }
      override def onJobEnd(jobEnd: SparkListenerJobEnd) {
        super.onJobEnd(jobEnd)
        println("ADAPT: Inside Job end Listener ");
        println("JobID " + jobEnd.jobId);
        println("EndTime " + jobEnd.time);
      }
    });

    sc.setCheckpointDir("/checkpoint-dir")

    // get threshold
    val threshold = args(1).toInt

    // read in text file and split each document into words
    val tokenized = sc.textFile(args(0)).flatMap(_.split(" "))

    tokenized.checkpoint()

    // tokenized.doCheckpoint()
    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)
    wordCounts.checkpoint()
    // wordCounts.doCheckpoint()
    // filter out words with fewer than threshold occurrences
    val filtered = wordCounts.filter(_._2 >= threshold)
    filtered.checkpoint()
    // filtered.doCheckpoint()
    // count characters
    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)
    charCounts.checkpoint()
    //  charCounts.doCheckpoint()
    System.out.println(charCounts.collect().mkString(", "))
  }
}
