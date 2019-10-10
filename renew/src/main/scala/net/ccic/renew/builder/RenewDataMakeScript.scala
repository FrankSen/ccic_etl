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
      |check(){
      |  if [ `grep "失败" ${argumentBuilder.outputpath}${argumentBuilder.retable}.log | wc -l` -gt 0 ];then
      |        echo "[`date '+%Y-%m-%d %T'`] Compute error, please check ${argumentBuilder.retable} log file."
      |        exit 1
      |  fi
      |}
      |
      |## truncate the log file, when run again.
      |cat /dev/null > ${argumentBuilder.outputpath}${argumentBuilder.retable}.log
      |
    """.stripMargin)

  def drawPostfix(command: String): String = " nohup " + command + " " + Seq(LEVELTWO).map(">" * _).mkString + s""" ${argumentBuilder.outputpath}${argumentBuilder.retable}.log & """

  def make(): Boolean = {

    val datelist = argumentBuilder.datelist
    var endScriptContent: String = null
    if (datelist == null || datelist.equals("")) {

      df.collect().foreach{
        row =>
          val shellCommand = drawPostfix(row.getAs[String]("EXEC_COMMAND")
            .replaceAll("##BEG_DATE##",argumentBuilder.begintime)
            .replaceAll("##END_DATE##", argumentBuilder.endtime))
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

      endScriptContent =
        s"""|${bufferScriptBoxBase.toString}
            |${if(bufferScriptBoxBase.toString.contains("bigdata_mis")) "wait;check" else "##"}
            |## level 3
            |${bufferScriptBoxLevelThree.toString}
            |## level 2
            |wait;check
            |${bufferScriptBoxLevelTwo.toString}
            |## level 1
            |wait;check
            |${bufferScriptBoxLevelOne.toString}
            |## level root
            |wait;check
            |${bufferScriptBoxLevelRoot.toString}
            |wait;check
            |
        |echo "[`date '+%Y-%m-%d %T'`] ${argumentBuilder.retable} Compute Success !!!"
      """.stripMargin

    }else{

      val redate = argumentBuilder.datelist
      if (!redate.contains(",")) throw new Exception("Error '--date-list' parameter")

      df.collect().foreach{
        row =>
          val shellCommand = drawPostfix(row.getAs[String]("EXEC_COMMAND")
            .replaceAll("##BEG_DATE##","\\$i")
            .replaceAll("##END_DATE##", "\\$(date -d \"\\$i 1 days\" '+%Y-%m-%d')"))
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

      endScriptContent =
        s"""|${bufferScriptBoxBase.toString}
            |${if(bufferScriptBoxBase.toString.contains("bigdata_mis")) "wait;check" else "##"}
            |
            |for i in ${argumentBuilder.datelist.split(",").mkString(" ")};do
            |## level 3
            |${bufferScriptBoxLevelThree.toString}
            |## level 2
            |wait;check
            |${bufferScriptBoxLevelTwo.toString}
            |## level 1
            |wait;check
            |${bufferScriptBoxLevelOne.toString}
            |## level root
            |wait;check
            |${bufferScriptBoxLevelRoot.toString}
            |wait;check
            |done
            |echo "[`date '+%Y-%m-%d %T'`] ${argumentBuilder.retable} Compute Success !!!"
      """.stripMargin

    }



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



