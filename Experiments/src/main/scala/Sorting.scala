
package experiments;

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object Sorting {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark Sort"))

    val lines = sc.textFile("data.txt")
    val rdd = lines
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    val sortedRDD = rdd.map(_.swap).sortByKey(false)
    val data = sortedRDD.take(5)


    System.out.println(sortedRDD.collect().mkString(", "))
  }
}
