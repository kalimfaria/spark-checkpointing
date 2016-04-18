/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples

import org.apache.spark.scheduler.{SparkListenerTaskStart, SparkListener, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 */
object SparkPageRank_Checkpointing {

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of PageRank and is given as an example!
        |Please use the PageRank implementation found in org.apache.spark.graphx.lib.PageRank
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }
    showWarning()
    val sparkConf = new SparkConf().setAppName("PageRank")
    val ctx = new SparkContext(sparkConf)
    //val sc = new SparkContext(sparkConf)
    ctx.addSparkListener(new SparkListener() {
      override def onJobStart(jobStart: SparkListenerJobStart) {
        super.onJobStart(jobStart)
        println("Inside Job-Start Listener");
        println("JobID: " + jobStart.jobId);
        val p = jobStart.properties
        println("Can't we just print the table: " + jobStart.properties)
        println("Keeping it a hashtable");
        for ((k,v) <- p) printf("key: %s, value: %s\n", k, v)
        println("Making it a map");
        val props = propertiesAsScalaMap(jobStart.properties)
        for ((k,v) <- props) printf("key: %s, value: %s\n", k, v)
        println("Job-Start Listener start-time: " + jobStart.time);
        println("End of Job-Start Listener");
       /* if (props.contains("spark.rdd.scope")) {
            if (props.contains("name") &&  props("name") == "checkpoint") {
              println("JobID " + jobStart.jobId);
              println("This is a checkpointing job for RDD - " + props("id"))
              println("StartTime - " + jobStart.time)
            }
        } */
      }

      override def onJobEnd(jobEnd: SparkListenerJobEnd) {
        super.onJobEnd(jobEnd)
        println("Inside Job-End Listener ");
        println("JobID: " + jobEnd.jobId);
        println("End Time: " + jobEnd.time);
        println("End of Job-End Listener ");
      }

    });
    val iters = if (args.length > 1) args(1).toInt else 10
    ctx.setCheckpointDir("/proj/CS525/faria/spark-temp/spark-checkpointing/spark/checkpoint-dir")
    val lines = ctx.textFile(args(0), 1)
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))

      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      println("Before the checkpoint \n")
     // println("Count of ranks:  " +  ranks.count())
      println("Iteration:  " +  i)
    //  val output = ranks.collect()
     // output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

      //ranks.persist()
     // ranks.doCheckpoint()
      ranks.cache()
      ranks.checkpoint()
      println("After the checkpoint \n")
      println("Count of ranks:  " +  ranks.count())

    }
    val output1 = ranks.collect()
    output1.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    ctx.stop()
  }
}
// scalastyle:on println
