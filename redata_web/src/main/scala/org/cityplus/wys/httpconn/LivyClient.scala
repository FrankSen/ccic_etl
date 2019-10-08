package org.cityplus.wys.httpconn

import java.util

import scala.collection.JavaConverters._
import com.alibaba.fastjson.{JSON, JSONException, JSONObject}

/**
  * @Description
  * @Author franksen
  * @CREATE 2019-10-05-下午11:32
  */
class LivyClient {

  private val HOSTURL = "http://pi:8998"

  def setSessionOfSpark(): Int = {
    try{
      val data = new JSONObject()

      data.put("kind", "spark")

      val res = HttpUtils.postAccess(HOSTURL + "/sessions", data)
      val session = JSON.parseObject(res)
      session.getIntValue("id")

    }catch {
      case e: JSONException =>
        throw new JSONException("Json maybe occur error.")
    }

  }

  def getJobInfo(id: Int): Unit ={
    val state = HttpUtils.getAccess(HOSTURL + "/sessions/" + id + "/statements/" + id)
    println(state.getString("state"))
  }

  def existsSessionNum(): Boolean = {
    val sessionState = HttpUtils.getAccess(HOSTURL + "/sessions")
    if (sessionState.getIntValue("total") > 0) true else false
  }

  def getIDLESessionId(): util.HashMap[String, Int] = {

    val sessionId = HttpUtils.getAccess(HOSTURL + "/sessions")
    val jsonArray = sessionId.getJSONArray("sessions")

    val ja: util.List[KindSparks] = JSON.parseArray(jsonArray.toJSONString, classOf[KindSparks])

    val _map = new util.HashMap[String, Int]()

    ja.asScala.map {
      kind =>
        (kind.state, kind.id)
    }.map(f => _map.put(f._1, f._2))

    _map
  }


  def dealSparkSql(id: Int): String ={
    val data = new JSONObject()
    data.put("code", "spark.sql(\"\"\"select 20 as age, \"wys\" as name \"\"\").show()")

    val sql = HttpUtils.postAccess(HOSTURL + "/sessions/" + id + "/statements", data)
    val sqlId = JSON.parseObject(sql).getIntValue("id")

    var resultJson = HttpUtils.getAccess(HOSTURL + "/sessions/" + id + "/statements/" + sqlId)
    var statu = resultJson.getString("output")

    println(statu)
    var result: String = null
    var temp: Boolean = true
    while(temp) {
      if(statu == null){
        Thread.sleep(2000)
        resultJson = HttpUtils.getAccess(HOSTURL + "/sessions/" + id + "/statements/" + sqlId)
        println(resultJson)
        statu = resultJson.getString("output")
        println(statu)
      }else{
        result  = resultJson.getJSONObject("output").getJSONObject("data").getString("text/plain")
        temp = false
      }

    }
    result

  }

}

case class OutputData(status: String)
case class KindSparks(id: Int, kind: String, state: String)
object LivyClient{

  def main(args: Array[String]): Unit = {

    val livy = new LivyClient

    if (livy.existsSessionNum()){
      val id = livy.getIDLESessionId().get("idle")

      println("Now live id exsts:" + id)
      println(livy.dealSparkSql(id))


    }else{

      val id = livy.setSessionOfSpark()
      println("Now live id:" + id)

    }
  }

}
