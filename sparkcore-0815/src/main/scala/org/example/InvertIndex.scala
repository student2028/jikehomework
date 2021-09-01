package org.example

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object InvertIndex {


  /**
   * output format
   * "is": {(0,2),{1,3}}
   * /Users/student2020/data/jike/spark
   */
  def main(args: Array[String]) = {

    val input = "/Users/student2020/data/jike/spark"

    /**
     * 首先获取路径下的文件列表，unionRDD 按照wordcount来构建
     */
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val fileList = fs.listFiles(new Path(input), true)

    var unionrdd = sc.emptyRDD[(String, String)]
    while (fileList.hasNext) {
      val path = new Path(fileList.next().getPath.toString)
      val fileName = path.getName
      println(path)
      unionrdd = unionrdd.union(sc.textFile(path.toString).flatMap(_.split("\\s+").map((fileName, _))))
    }

    println("---" * 100)
    unionrdd.foreach(println)
    println("---" * 100)

    //构造词频
    val rdd = unionrdd.map(word => {
      (word, 1)
    }).reduceByKey(_ + _)
    println("---" * 100)
    rdd.foreach(println)
    println("---" * 100)

    //调整输出格式
    val frdd = rdd.map(data => {
      (data._1._2, String.format("(%s,%s)", data._1._1, data._2.toString))
    }).reduceByKey(_ + "," + _)
      .map(word => String.format("\"%s\",{%s}", word._1, word._2))


    println("---" * 100)
    frdd.foreach(println)
    println("---" * 100)

  }

}
