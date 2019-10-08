package net.ccic.etl.utils

import java.io.{File, FileInputStream, IOException, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.Properties

import net.ccic.etl.EtlException

import scala.collection.JavaConverters._
import org.apache.spark.Logging
import java.lang.System.getenv

import net.ccic.etl.submit.REData
import net.ccic.etl.submit.REData.REData

/**
  * @Description
  * @Author franksen
  * @CREATE 2019-08-19-下午3:08
  */
private[ccic] object Utils extends Logging {

  def trimExceptCRLF(str: String): String = {
    val nonSpaceOrNaturalLine: Char => Boolean ={ ch =>
      ch > ' ' || ch == '\r' || ch == '\n'
    }

    val firstPos = str.indexWhere(nonSpaceOrNaturalLine)
    val lastPos = str.lastIndexWhere(nonSpaceOrNaturalLine)

    if(firstPos >= 0 && lastPos >= 0){
      str.substring(firstPos, lastPos+1)
    }else{
      ""
    }
  }

  def getPropertiesFromFile(filename: String):Map[String, String] = {
    val file = new File(filename)

    require(file.exists(), s"Properties file $file dose not exist.")
    require(file.isFile,s"Properties file $file is not a usually file.")

    val inReader = new InputStreamReader(new FileInputStream(file),StandardCharsets.UTF_8)

    try{

      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().asScala
        .map{k => (k,trimExceptCRLF(properties.getProperty(k)))}
        .toMap
    }catch {
      case e :IOException =>
        throw new EtlException(s"Failed when loading Etl properties from $file .",e)
    }finally {
      inReader.close()
    }
  }

  def getSysEnvByName(reDataType: REData): String ={
    reDataType match {
      case REData.USERNAME =>
        sys.env("OR_USERNAME")

      case REData.PASSWORD =>
        sys.env("OR_PASSWORD")

      case REData.URL =>
        sys.env("OR_URL")

      case _ =>
        throw new EtlException("Dot`t match Redata type")
    }
  }

  def getSparkClassPath(): String ={
    val classPath = getenv("SPARK_HOME")
    require(classPath != null, "Don`t set the application home, So you need to set APP_HOME in System env.")
    classPath + File.separator + "jars/*"
  }

  def classForName[C](
                       className: String
                       ,initialize: Boolean = true): Class[C] ={
    assert(className.nonEmpty, "The className can`t empty, so you need add class with '--class'.")
    Class.forName(className, initialize, Thread.currentThread().getContextClassLoader)
      .asInstanceOf[Class[C]]
  }
}
