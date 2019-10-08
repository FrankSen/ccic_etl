package net.ccic.etl.submit

import java.{io, util}

import net.ccic.etl.launcher.EtlSubmitArgumentsParser
import net.ccic.etl.utils.Utils
import net.ccic.etl.{EtlException, submit}
import org.apache.spark.Logging

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
  * @Description
  * @Author franksen
  * @CREATE 2019-08-19-下午2:38
  */
private class EtlSubmitArgument(args: Seq[String], env:Map[String,String] = sys.env)
               extends EtlSubmitArgumentsParser with Logging{

  var mainClass: String = null
  var sqlText: String = null
  var runMode: String = null
  var appName: String = null
  var srcName: String = null
  var primKey: String = null
  var userName: String = null
  var passWord: String = null
  var url: String = null
  var reTable: String = null
  var beginTime: String = null
  var endTime: String = null
  var dateList: String = null
  var outputPath: String = null
  var interval: String = null


  private  val INTO: String = "INTO"
  private  val OVERWRITE: String = "OVERWRITE"
  private  val MERGE_BY_TABLE: String = "MERGE_BY_TABLE"
  private  val MERGE_BY_SQL: String = "MERGE_BY_SQL"
  private  val PARTITION: String = "PARTITION"
  private  val REDATA: String = "REDATA"

  var verbose: Boolean = false
  var childArgs: ArrayBuffer[String] = new ArrayBuffer[String]()
  var action: Comparable[_ >: submit.EtlSubmitAction.Value with String <: Comparable[_ >: submit.EtlSubmitAction.Value with String] with io.Serializable] with io.Serializable = null

  lazy val defaultEtlProperties: HashMap[String, String] ={
    val defaultProperties = new mutable.HashMap[String, String]()
    Option(System.getenv("DEFAULT_PROPERTY_FILE")).foreach{ filename =>
      val properties = Utils.getPropertiesFromFile(filename)
      properties.foreach{case (k, v) =>
          defaultProperties(k) = v
      }

      if(verbose){
        properties.foreach{case(k, v) =>
            logInfo(s"Adding default property: $k=$v")
        }
      }
    }
    defaultProperties
  }

  parse(args.asJava)


  loadEnvironmentArguments()


  validateArguments()

  /**
    * Load arguments from current environment, etl properties etc.
    */
  private  def loadEnvironmentArguments() = {


  }


  private def validateArguments() = {

    action = runMode match {

      case INTO =>
        EtlSubmitAction.INTO

      case OVERWRITE =>
        EtlSubmitAction.OVERWRITE

      case MERGE_BY_SQL =>
        EtlSubmitAction.MERGE_BY_SQL

      case MERGE_BY_TABLE =>
        EtlSubmitAction.MERGE_BY_TABLE

      case PARTITION =>
        EtlSubmitAction.PARTITION

      case REDATA =>
        EtlSubmitAction.REDATA

      case _ =>
        logInfo("Error: Don`t Match mode, please choose mode with '--run-mode' option.")
        EtlSubmitAction.PRINT_USAGE
    }

  }


  override def toString: String = {
    s"""
      |Parsed arguments:
      |   appName             $appName
      |   sqlTest             $sqlText
      |   srcName             $srcName
      |   verbose             $verbose
      |   primKey             $primKey
      |   runMode             $runMode
      |   userName            $userName
      |   passWord            $passWord
      |   url                 $url
      |   reTable             $reTable
      |   beginTime           $beginTime
      |   endTime             $endTime
      |   dateList            $dateList
      |   outputPath          $outputPath
      |   interval            $interval
      |
    """.stripMargin
  }

  override protected def handle(opt: String, value: String): Boolean = {
    opt match {
      case CLASS =>
        mainClass = value

      case APPNAME =>
        appName = value

      case SQLTEXT =>
        sqlText = value

      case SRCNAME =>
        srcName = value

      case PRIMKEY =>
        primKey = value

      case RUN_MODE =>
        //change to uppercase for runMode
        runMode = value.toUpperCase

      case VERBOSE | VB =>
        verbose = true

      case USERNAME | USER =>
        userName = value

      case PASSWORD | PW =>
        passWord = value

      case URL =>
        url = value

      case RETABLE =>
        reTable = value

      case OUTPUTFILE | OUT =>
        outputPath = value

      case DATELIST =>
        dateList = value

      case INTERVAL =>
        interval = value

      case BEGINTIME | BEGIN =>
        beginTime = value

      case ENDTIME | END =>
        endTime = value

      case HELP | USAGE_ERROR | HLP  =>
        action = EtlSubmitAction.PRINT_USAGE

      case _ =>
        error("Unexpected argument: %s".format(opt))
        action = EtlSubmitAction.PRINT_USAGE

    }
    action != EtlSubmitAction.PRINT_USAGE
  }

  override protected def handleExtraArgs(extra: util.List[String]): Unit = {
    childArgs ++= extra.asScala
  }

  override protected def handleUnknown(opt: String): Boolean = {

    if(opt.startsWith("-")){
      error(s"Unrecognized option '$opt'.")
    }
    false
  }


  private def error(msg: String): Unit = throw new EtlException(msg)
}
