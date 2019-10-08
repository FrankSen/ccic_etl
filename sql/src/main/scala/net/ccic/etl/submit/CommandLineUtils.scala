package net.ccic.etl.submit

import java.io.PrintStream

/**
  * @Description
  * @Author franksen
  * @CREATE 2019-08-19-下午2:15
  */
 trait CommandLineUtils extends CommandLineLoggingUtils {
  def main(args: Array[String]): Unit
}

 trait CommandLineLoggingUtils{
   var exitFn: Int => Unit = (exitCode: Int) => System.exit(exitCode)

   var printStream :PrintStream = System.err

   def printMessage(str: String): Unit = printStream.println(str)

   def printErrorAndExit(str: String): Unit = {
    printMessage("Error" + str)
    printMessage("Run with --help for usage help or --verbose for debug output")
    exitFn(1)
  }


}
