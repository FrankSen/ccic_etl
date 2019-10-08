package net.ccic.renew.builder

/**
  * @Description
  * @Author franksen
  * @CREATE 2019-09-16-下午4:00
  */
class RenewDataArgumentBuilder(args: Seq[String]) extends AbstractRenewDataArgBuilder {

  var username: String = null
  var password: String = null
  var url: String = null
  var retable: String = null
  var begintime: String = null
  var endtime: String = null
  var datelist: String = null
  var outputpath: String = null
  var interval: String = null
  private[renew] final val database: String = "ODS2_INTEG"

  assert(args.size == 9, "RenewData parameter less, check your argument.")

 //set the args value to local parameter.

  val setParaBox: Array[String] = Array(USERNAME, PASSWORD, URL, RETABLE, BEGINTIME, ENDTIME, OUTPUTPATH, DATELIST, INTERVAL)

  val consistCollect: Array[(String, String)] = setParaBox zip args

  consistCollect.foreach{
    p => p._1 match {

      case USERNAME =>
        this.username = p._2

      case PASSWORD =>
        this.password = p._2

      case URL =>
        this.url = p._2

      case RETABLE =>
        this.retable = p._2

      case BEGINTIME =>
        this.begintime = p._2

      case ENDTIME =>
        this.endtime = p._2

      case OUTPUTPATH =>
        this.outputpath = p._2

      case DATELIST =>
        this.datelist = p._2

      case INTERVAL =>
        this.interval = p._2

      case _ =>
        throw new IllegalArgumentException(s"Error parameter: ${p._1}, check your input.")

    }
  }
}
