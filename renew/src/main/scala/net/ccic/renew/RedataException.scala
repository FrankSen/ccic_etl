package net.ccic.renew

/**
  * @Description
  * @Author franksen
  * @CREATE 2019-09-20-下午3:50
  */
class RedataException(message: String, cause: Throwable)
    extends Exception(message, cause){

  def this(message: String)  = this(message, null)

}

private case class RedataArgumentConflictException() extends RedataException("Arguments option  cat`t be present at the same time.")

private case class RedataArgumentTgtTableNameException(message: String) extends RedataException(message)

private case class RedataArgumentTgtTableNameIllegalException(message: String) extends RedataException(message)