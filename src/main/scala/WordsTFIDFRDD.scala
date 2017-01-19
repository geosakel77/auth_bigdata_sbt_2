/**
  * Created by george on 19/1/2017.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.MLUtils

object WordsTFIDFRDD {

  def tfidfrdd(sc: SparkContext,inputFile:String,splitRate:Array[Double]): Unit = {
    val hashingTF = new HashingTF()

    val trainData = sc.textFile(inputFile).map(line =>LabeledPoint.apply(line.split(",")(1).toDouble,hashingTF.transform(line.split(",")(0).replace("\"","").split(" "))))

    trainData.foreach(row =>print(row))
  }


  def main(args: Array[String]): Unit = {
    var csvfilename: String = ""
    if (args.length != 0) {
      csvfilename = args(0)
      if (!(csvfilename == "--help")) {

        val conf = new SparkConf().setAppName("ML Auth App").setMaster("local[1]")
        val sc = new SparkContext(conf)
        val currentDir = System.getProperty("user.dir")
        println(currentDir)
        val inputFile = "file://" + currentDir + "/" + csvfilename
        println(inputFile)
        this.tfidfrdd(sc,inputFile,Array(0.6,0.4))


      } else {
        println("scala WordsTfIdf.scala csv_file_name")
      }

    } else {
      println("Please provide file path.")

    }
  }

}
