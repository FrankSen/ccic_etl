package net.ccic.renew.util

import java.io.PrintStream
import java.sql.DriverManager

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description
  * @Author franksen
  * @CREATE 2019-09-10-下午6:50
  */
object ConnectUtils {

  var printStream :PrintStream = System.err

  def spark = SparkSession
                                     .builder()
                                     .master("local")
                                     .appName("CONN_ORACLE_TEST")
                                     .config("spark.sql.warehouse.dir", "/tmp/warehouse")
                                     .getOrCreate()

  def oracleConnect(username: String, password: String, url: String)={
    Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
    DriverManager.getConnection(url, username, password)
  }

  def printMessage(msg: String): Unit = printStream.println(msg)

}

