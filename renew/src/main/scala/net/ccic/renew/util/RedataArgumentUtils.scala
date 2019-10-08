package net.ccic.renew.util

import java.io.PrintStream

import net.ccic.renew.builder.RenewDataArgumentBuilder

/**
  * @Description
  * @Author franksen
  * @CREATE 2019-09-20-下午2:33
  */
trait RedataArgumentUtils extends RedataArgumentLoggingUtils {
  def checkManage(obj: RenewDataArgumentBuilder): Unit
}

trait RedataArgumentLoggingUtils{

  var exitInt: Int => Unit = (exitNum: Int) => System.exit(exitNum)

  val printStream: PrintStream = System.err

  def printMessage(msg: String): Unit = printStream.println(msg)

  def printErrorAndExit(str: String): Unit = {
    printMessage("Error:" + str)
    printMessage("Run with --help or --verbose to show all args of values.")
    exitInt(1)
  }

}


