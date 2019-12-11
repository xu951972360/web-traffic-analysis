package com.xu.preparser

import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import com.xu.prepaser.{PreParsedLog, WebLogPreParser}
import org.apache.spark.SparkConf

object PreparserETL {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    if(args.isEmpty){
      conf.setMaster("local")
    }
    val spark = SparkSession.builder()
      .appName("PreparserETL")
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()
    val rawDataPath = spark.conf.get("spark.traffic.analysis.rawdata.input","hdfs://master:9999/user/hadoop-twq/traffic-analysis/rawlog/20180615")
    val numPartition = spark.conf.get("spark.traffic.analysis.rawdata.numberPartitons","2").toInt
    val preParsedRDD=spark.sparkContext.textFile(rawDataPath)
      .flatMap(line=>Option(WebLogPreParser.parse(line)))
    val preParsedDS=spark.createDataset(preParsedRDD)(Encoders.bean(classOf[PreParsedLog]))
    preParsedDS.repartition(numPartition).
      write.mode(SaveMode.Append).saveAsTable("rawdata.web")

    spark.stop()

  }

}
