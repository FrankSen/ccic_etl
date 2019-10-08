package net.ccic.etl

/**
  * @Description
  * @Author franksen
  * @CREATE 2019-08-19-下午3:53
  */
class EtlException(message: String, cause: Throwable)
  extends Exception(message,cause){

  def this(message: String) = this(message, null)

}

/**
  * Exception throw when the relative user app execute occur error.
  * @param exitCode
  */
private case class EtlUserAppException(exitCode: Int) extends EtlException(s"User application exited with $exitCode")


