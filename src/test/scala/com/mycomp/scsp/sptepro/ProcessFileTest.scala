package com.mycomp.scsp.sptepro

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}
import org.scalatest.junit.AssertionsForJUnit

class ProcessFileTest extends AssertionsForJUnit {

  var sparkContext: SparkContext = _

  @Before
  def initialize() = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("testscalaspark")
    sparkContext = new SparkContext(conf)
  }

  @After
  def tearDown() = {
    sparkContext.stop()
  }

  @Test
  def testFileProcessor(): Unit = {
    val fileProcessor = new ProcessFile(sparkContext)
    val wordsCountMap = fileProcessor.run("resources/input.txt")
    assert(wordsCountMap("at") == 9)
    assert(wordsCountMap("Lorem") == 1)
  }
}
