package net.ccic.renew.builder

import java.io.{File, PrintWriter}
import java.util.Date

import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
/**
  * @Description
  * @Author franksen
  * @CREATE 2019-09-17-上午9:43
  */
class RenewDataMakeScript(df:DataFrame, argumentBuilder: RenewDataArgumentBuilder) extends  Logging{

  private  var script_name: Array[String] = null
  private  var level: Int = 0
  private  var end_level: Int = 0
  private  var redata_time: String = null
  private  var target_table: String = null

  private final val LEVELZERO: Int = 0
  private final val LEVELONE: Int = 1
  private final val LEVELTWO: Int = 2
  private final val LEVELTHREE: Int = 3
  private var printWriter: PrintWriter = null
  private var isOccurError: Boolean = false

  val bufferScriptBoxBase : StringBuffer = new StringBuffer("#!/bin/bash" + "\n"*2)
  val bufferScriptBoxLevelRoot: StringBuffer = new StringBuffer()
  val bufferScriptBoxLevelOne: StringBuffer = new StringBuffer()
  val bufferScriptBoxLevelTwo: StringBuffer = new StringBuffer()
  val bufferScriptBoxLevelThree: StringBuffer = new StringBuffer()

  bufferScriptBoxBase.append(
    s"""
      |###########################
      |## Create by: Wys
      |## Generate Data: ${String.format("%1$tF %1$tT", new Date(System.currentTimeMillis()))}
      |## Redata of table: ${argumentBuilder.retable.replaceAll("CCIC_EDW_","CCIC_EDW.")}
      |############################
    """.stripMargin)

  def make(): Boolean = {

    if (argumentBuilder.datelist.isEmpty) {
      df.collect().foreach{
        row =>
          val shellCommand = row.getAs[String]("EXEC_COMMAND")
            .replaceAll("##BEG_DATE##",argumentBuilder.begintime)
            .replaceAll("##END_DATE##", argumentBuilder.endtime)
          level = row.getAs[Int]("END_RANK") ;level match {

          case LEVELZERO =>
            bufferScriptBoxLevelRoot.append(shellCommand + "\n")

          case LEVELONE =>
            bufferScriptBoxLevelOne.append(shellCommand + "\n")

          case LEVELTWO =>
            bufferScriptBoxLevelTwo.append(shellCommand + "\n")

          case LEVELTHREE =>
            bufferScriptBoxLevelThree.append(shellCommand + "\n")

          case _ =>
            bufferScriptBoxBase.append(s"\n ## Confirm whether the layer perform artificial: $level, Check source data. \n").append( s"## ${shellCommand} \n")

        }

      }

    }else{

    }

    val endScriptContent =
      s"""|${bufferScriptBoxBase.toString}
        |
        |${if(bufferScriptBoxBase.toString.contains("bigdata_mis")) "wait" else "##"}
        |## level 3
        |${bufferScriptBoxLevelThree.toString}
        |## level 2
        |wait
        |${bufferScriptBoxLevelTwo.toString}
        |## level 1
        |wait
        |${bufferScriptBoxLevelOne.toString}
        |## level root
        |wait
        |${bufferScriptBoxLevelRoot.toString}
      """.stripMargin

    try{
      printWriter = new PrintWriter(new File(argumentBuilder.outputpath + File.separator + argumentBuilder.retable + ".sh"))
      printWriter.write(endScriptContent)

    }catch {
      case e: Throwable =>
        logError("Write shell script file occur error.", e)
        isOccurError = true
    }finally {
      printWriter.close()
    }

    isOccurError

  }






}



