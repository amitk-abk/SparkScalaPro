package com.mycomp.scsp.sptepro

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.Map

class ProcessFile(sparkContext: SparkContext) {
  private val context = sparkContext

  private def readFileAndGetLinesRDD(path: String): RDD[String] = {
    return context.textFile(path)
  }

  private def processLinesRDDToGetWordsRDD(linesRDD: RDD[String]) : RDD[String] = {
    return linesRDD.flatMap(line => line.split(" ")).filter(line => !line.isEmpty)
  }

  private def getWordsCountFromRDD(wordsRDD: RDD[String]): Map[String, Long] = {
    return wordsRDD.countByValue()
  }

  def run(path: String): Map[String, Long] = {
    val linesRDD = readFileAndGetLinesRDD(path)
    val wordsRDD = processLinesRDDToGetWordsRDD(linesRDD)
    return getWordsCountFromRDD(wordsRDD)
  }

  def printResultFromWordsRDD(wordCounts: Map[String, Long]): Unit = {
    for ((word, count) <- wordCounts) println(word + " " + count)
  }
}

object ProcessFile {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("sparktest")

    val context = new SparkContext(conf)
    val fileProcessor = new ProcessFile(context)

    val wordsCount = fileProcessor.run("resources/input.txt")
    fileProcessor.printResultFromWordsRDD(wordsCount)
  }
}
