package net.ccic.etl.submit

import net.ccic.etl.EtlUserAppException
import net.ccic.etl.launcher.{EtlApplication, JavaMainApplication}
import net.ccic.etl.submit.EtlSubmit.{isRedata, printMessage}
import net.ccic.etl.utils.Utils._
import org.apache.spark.Logging
import scala.collection.mutable.ArrayBuffer

/**
  * @Description
  * @Author franksen
  * @CREATE 2019-08-19-下午2:12
  */
private class EtlSubmit extends Logging {

  private var appArgs: EtlSubmitArgument = null


  def partition(): Unit = {

  }

  def bysql(appArgs: EtlSubmitArgument): Unit = {

  }

  def into(): Unit = {

  }

  def overwrite(): Unit = {


  }



  def runMain(args: EtlSubmitArgument): Unit = {
    val (childArgs, childClasspath, etlConf, childMainClass) = prepareSubmitEnvironment(args)

    var mainClass: Class[_] = null

    try{

      mainClass = classForName(childMainClass)
    }catch {
      case e: ClassNotFoundException =>
        logError(s"Failed to load class $childMainClass.")
        throw new EtlUserAppException(110)

      case e: NoClassDefFoundError =>
        logError(s"Failed to load $childMainClass: ${e.getMessage}")
        throw new EtlUserAppException(110)
    }

    val app: EtlApplication = new JavaMainApplication(mainClass)

    app.start(childArgs.toArray)


  }
  private def redata(args: EtlSubmitArgument): Unit = {

    if (args.verbose) {

      logInfo(args.toString)
    }

    logInfo(
      """  Welcome to
        |   ______________________     ____    ____
        |  / ____/ ____/  _/ ____/    / __ )  /  _/
        | / /   / /    / // /  ______/ /_/ /  / /
        |/ /___/ /____/ // /__/_____/ /_/ /  / /
        |\____/\____/___/\____/    /_____/ /___/
        |
      """.stripMargin)
    runMain(args)

  }

  def prepareSubmitEnvironment(args: EtlSubmitArgument): (ArrayBuffer[String], ArrayBuffer[String], String, String) = {

    //Return values
    val childArgs = new ArrayBuffer[String]()
    val childClasspath = new ArrayBuffer[String]()
    var etlConf = ""
    var childMainClass = ""

    //fill in args that base param
    childArgs += Option(args.userName).getOrElse(getSysEnvByName(REData.USERNAME))
    childArgs += Option(args.passWord).getOrElse(getSysEnvByName(REData.PASSWORD))
    childArgs += Option(args.url).getOrElse(getSysEnvByName(REData.URL))
    childArgs += args.reTable
    childArgs += args.beginTime
    childArgs += args.endTime
    childArgs += Option(args.outputPath).getOrElse("./")
    childArgs += args.dateList
    childArgs += args.interval

    //Set the mainClass
    childMainClass = args.mainClass

    //Set the class path

    childClasspath += getSparkClassPath()

    (childArgs, childClasspath, etlConf, childMainClass)
  }

  def doSubmit(args: Array[String]):Unit = {

      this.appArgs = parseArgument(args)

       if(appArgs.verbose){
         logInfo(appArgs.toString)
       }

       appArgs.action match {
             case EtlSubmitAction.MERGE_BY_SQL => bysql(appArgs)
             case EtlSubmitAction.INTO => into()
             case EtlSubmitAction.OVERWRITE => overwrite()
             case EtlSubmitAction.PARTITION => partition()
             case EtlSubmitAction.PRINT_USAGE => printUsage()
             case EtlSubmitAction.REDATA => redata(appArgs)
             case _ => printUsage()
       }

  }

  protected def parseArgument(args: Array[String]): EtlSubmitArgument = {
    new EtlSubmitArgument(args)
  }


  def printUsage(): Unit = {
    printMessage(
      s"""
         |Options:
         |  --run-mode RUN_MODE         Whether to run the mode that about "INTO" "OVERWRITE" or
         |                              merge data by sql("MERGE_BY_SQL") , deal with the data
         |                              to partition("PARTITION"), refresh data by("REDATA").
         |  --class    CLASS_NAME       Your application's main class (for Java / Scala apps).
         |  --name     APPNAME          A YARN name of your application.
         |  --src      CFG FILE         A source property file that deploy environment for the application.
         |
         |  --packages                  Comma-separated list of maven coordinates of jars to include
         |                              on the driver and executor classpaths. Will search the local
         |                              maven repo, then maven central and any additional remote
         |                              repositories given by --repositories. The format for the
         |                              coordinates should be groupId:artifactId:version.
         |
         |  --sql    SQL_LANUAGE        SELECT, INERT, CREATE and so on.
         |
      """.stripMargin
    )

    if (isRedata(appArgs.mainClass)){
      logInfo("Redata module:")
      logInfo(
        """
          |
          |  Redata Options:
          |  -u, --username           oracle account name.
          |  -p, --password           oracle account password.
          |  --url                    xx.xx.xx.xxx:1521:orcl
          |  --rt                   * the name of refresh table.
          |  -s, --begintime        * Start time of refresh data.
          |  -e, --endtime          * End time of refresh data. together with option '-s',have to option '--interval'.
          |  -o, --outputPath       * the script of redata`s output path.
          |  --date-list              [xxxx-xx-xx] you want to any date that table of refresh.
          |  --interval               [day/month] Partition interval for the table.
          |
        """.stripMargin)
    }
  }
}

object EtlSubmit extends CommandLineUtils with Logging{



  override def main(args: Array[String]): Unit = {
    val submit = new EtlSubmit(){
      self =>
      override def parseArgument(args: Array[String]): EtlSubmitArgument = {
        new EtlSubmitArgument(args){
          override protected def logInfo(msg: => String): Unit = self.logInfo(msg)

          override protected def logWarning(msg: => String): Unit = self.logWarning(msg)

          override protected def logError(msg: => String): Unit = self.logError(msg)
        }
      }

      override protected def logInfo(msg: => String): Unit = printMessage(msg)

      override protected def logWarning(msg: => String): Unit = printMessage(s"Warning: $msg")

      override protected def logError(msg: => String): Unit = printMessage(s"Error: $msg")

      override def doSubmit(args: Array[String]): Unit = {
        try{
          super.doSubmit(args)
        } catch{
          case e: EtlUserAppException =>
            exitFn(e.exitCode)
        }
      }
    }

    submit.doSubmit(args)

  }

  private def isRedata(mainClass: String):Boolean = {
    mainClass == "net.ccic.renew.dao.RenewDataSQL"
  }



}

object EtlSubmitAction extends Enumeration{
  type EtlSubmitAction = Value
  val MERGE_BY_SQL,
      MERGE_BY_TABLE,
      INTO,
      OVERWRITE,
      PARTITION,
      REDATA,
      PRINT_USAGE = Value
}

object REData extends Enumeration {
  type REData = Value

  val USERNAME,
      PASSWORD,
      URL,
      BEGINTIME,
      ENDTIME = Value
}
