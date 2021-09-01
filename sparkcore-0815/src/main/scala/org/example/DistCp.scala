package org.example

import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object DistCp {

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //    val input = "/Users/student2020/jike"
    //    val output = "/Users/student2020/jike_tgt"


    //parse args
    val options = new Options()
    options.addOption("i", "ignore failure ", false, "ignore failures")
    options.addOption("m", "max concurrence ", true, "max concurrence")
    val parser = new DefaultParser()
    val cmd = parser.parse(options, args)

    if (args.length < 2) {
      println("DistCP -m 10 -i input output ")
      System.exit(-1)
    }

    val input = args(args.length - 2)
    val output = args(args.length - 1)


    val IGNORE_FAILURE = cmd.hasOption("i")
    val MAX_CONNCURRENCE = if (cmd.hasOption("m")) cmd.getOptionValue("m").toInt
    else 2

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val fileList = fs.listFiles(new Path(input), true)

    val arrayBuffer = ArrayBuffer[String]()
    while (fileList.hasNext) {
      val path = fileList.next().getPath.toString
      arrayBuffer.append(path)
      println(path)
    }


    val rdd = sc.parallelize(arrayBuffer, MAX_CONNCURRENCE)
    rdd.foreachPartition(it => {
      val conf = new Configuration()
      val fs = FileSystem.get(conf)

      while (it.hasNext) {
        val src = it.next()
        val tgt = src.replace(input, output)
        try {
          FileUtil.copy(fs, new Path(src), fs, new Path(tgt), false, conf)
        } catch {
          case ex: Exception =>
            if (IGNORE_FAILURE) println("ignore failure when copy")
            else throw ex
        }
      }


    })


  }

}
