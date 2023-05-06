package com.one.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit._

class TransformationTest {

  private var sparkContext: SparkContext = _

  @Before
  def init(): Unit = {
    val conf = new SparkConf().setAppName("TransformationTest").setMaster("local")
    this.sparkContext = new SparkContext(conf)
  }

  @Test
  def mapTest(): Unit = {
    val inputRDD = sparkContext.parallelize(Array[(Int, Char)](
      (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (2, 'e'), (3, 'f'), (2, 'g'), (1, 'h')
    ), 3)
    val resultRDD = inputRDD.map(r => r._1 + "_" + r._2)
    resultRDD.foreach(println)
  }

  @Test
  def mapValues(): Unit = {
    val inputRDD = sparkContext.parallelize(Array[(Int, Char)](
      (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (2, 'e'), (3, 'f'), (2, 'g'), (1, 'h')
    ), 3)
    val resultRDD = inputRDD.mapValues(x => x + "_1")
    resultRDD.foreach(println)
  }
}
