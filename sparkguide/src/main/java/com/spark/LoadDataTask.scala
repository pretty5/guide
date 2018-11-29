package com.spark

import java.util.{Properties, UUID}

import com.common.Constants
import org.apache.spark.sql.SparkSession

object LoadDataTask {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master(Constants.MASTER)
      .appName(Constants.APP_NAME)
      .getOrCreate()

    //val tmpDir= Constants.HOME + "\\" + UUID.randomUUID().toString();

    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("dbtable","guide_hospital")
      .option("user","root")
      .option("password","123456")
      .load()
      .write.text(args(0))
  }

}
