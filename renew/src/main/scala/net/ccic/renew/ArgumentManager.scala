package net.ccic.renew

import net.ccic.renew.builder.RenewDataArgumentBuilder
import net.ccic.renew.util.RedataArgumentUtils
import org.apache.spark.Logging

/**
  * @Description
  * @Author franksen
  * @CREATE 2019-09-20-下午2:17
  */
class ArgumentManager extends Logging {
  //

  def start (obj: RenewDataArgumentBuilder)={

    if (obj.begintime != null && obj.begintime != "" && obj.datelist != null && obj.datelist != ""){

      logError(s"Error: -s ${obj.begintime} and --data-list ${obj.datelist} choose one only.")
      throw new RedataArgumentConflictException

    }

    if (!obj.retable.startsWith("CCIC_EDW")){

      logWarning(s"May be --rt ${obj.retable} isn`t belong to database of CCIC_EDW.")
      throw RedataArgumentTgtTableNameException("Target table name isn`t CCIC_EDW.")

    }

    if(obj.retable.contains("\\.")){

      logError(s"Option --rt ${obj.retable} don`t contain '.' flag.")
      throw RedataArgumentTgtTableNameIllegalException("Target table contain illegal argument.")

    }

  }

}

object ArgumentManager extends RedataArgumentUtils with Logging {

  override def checkManage(obj: RenewDataArgumentBuilder): Unit = {

    val argManager = new ArgumentManager {

      override protected def logInfo(msg: => String): Unit = printMessage(msg)

      override protected def logError(msg: => String): Unit = printMessage(msg)

      override protected def logWarning(msg: => String): Unit = printMessage(msg)

      override def start(obj: RenewDataArgumentBuilder): Unit = {

          super.start(obj)

      }

    }
    argManager.start(obj)

  }
}