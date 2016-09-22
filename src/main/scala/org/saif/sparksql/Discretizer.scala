package org.saif.sparksql

/**
  * Created by saif on 03/18/15
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.feature.MDLPDiscretizer
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.DenseVector

object Discretizer {

  def vectorValueExpander(vector_positions: Array[Int], row: Row): Array[Double] = {
    val row_expansion: Array[Double] = for (l: Int <- vector_positions) yield row.getDouble(l)
    row_expansion
  }

  def dfLabelize(df: DataFrame, label_index: Int): RDD[LabeledPoint] = {
    val column_size: Int = df.columns.length
    val vectorPositions: Array[Int] = Array((1 until label_index) ++ ((label_index + 1) until column_size)).flatten
    val labelledDf: RDD[LabeledPoint] = df.map( row =>
      new LabeledPoint(
        row.getDouble(label_index),
        new DenseVector(vectorValueExpander(vectorPositions, row))
      )
    )
    labelledDf
  }

  def dfGetNumerical(df: DataFrame): DataFrame = {
    val numericalColumnNames = df.dtypes.filter(_._2 == DoubleType).map(_._1)
    val numericalColumns: Array[Column] = numericalColumnNames.map(df.col)
    df.select(numericalColumns: _*)
  }

  def rddDiscretize(rdd: RDD[LabeledPoint]) = {

    val categoricalFeat: Option[Seq[Int]] = None
    val nBins = 25
    val maxByPart = 10000

    rdd.persist(StorageLevel.MEMORY_AND_DISK_SER).count

    val discretizer = MDLPDiscretizer.train(
      rdd,
      categoricalFeat,
      nBins,
      maxByPart
    )

    rdd.map(row => new LabeledPoint(row.label, discretizer.transform(row.features)))

  }

  def main(args: Array[String]) {

    val conf = new SparkConf().
      setAppName("Discretizer").
      set("spark.executor.memory", "5g").
      set("spark.driver.memory", "8g").
      set("spark.storage.memoryFraction", "0.4").
      set("spark.default.parallelism", "24").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      registerKryoClasses(Array(classOf[RDD[LabeledPoint]]))
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val data = sqlContext.
      read.
      format("com.databricks.spark.csv").
      option("header", "true").
      option("inferSchema", "true").
      load(args.headOption.getOrElse(throw new IllegalArgumentException("please submit a csv file as argument")))
    val numerical_data = dfGetNumerical(data)
    val labelledData = dfLabelize(numerical_data, 1)
    val discretizedData = rddDiscretize(labelledData)
    discretizedData.take(10).foreach(println(_))
    sc.stop

  }
}
