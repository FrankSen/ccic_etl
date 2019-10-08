package net.ccic.renew.dao

import java.sql.{Connection, ResultSet}

import net.ccic.renew.ArgumentManager
import net.ccic.renew.builder.{RenewDataArgumentBuilder, RenewDataMakeScript}
import net.ccic.renew.util.ConnectUtils._
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.Row

/**
  * @Description
  * @Author franksen
  * @CREATE 2019-09-10-下午7:12
  */
case class ResultData(REF_ETL_OBJ_ID: String, END_RANK: Int, PM_VALUE: String)

object RenewDataSQL{

  private final var argBuilder: RenewDataArgumentBuilder = _
  var showData: Array[Row] = _

  def main(args: Array[String]): Unit = {

    argBuilder = new RenewDataArgumentBuilder(args)

    ArgumentManager.checkManage(argBuilder)

    connectGenJdbcRddBySql()

  }

  def init(args: Array[String]): Array[Row] = {
    argBuilder = new RenewDataArgumentBuilder(args)
    ArgumentManager.checkManage(argBuilder)
    connectGenJdbcRddBySql()
    showData
  }

  def extractValues(r: ResultSet): (String, Int, String) = {
    (r.getString(1)
      , r.getInt(2)
      ,r.getString(3))
  }


  def createConnection(): Connection = {
    oracleConnect(argBuilder.username,argBuilder.password, argBuilder.url)
  }

  private def connectGenJdbcRddBySql(): Unit = {
    val dealDataThings =
      s"""
        |
        |
        |WITH TAB AS
        |  (SELECT DISTINCT AA.REF_ETL_OBJ_ID ,
        |                   CASE
        |                       WHEN SUBSTR(AA.DEP_ETL_OBJ_ID,1,4) = 'J_C_' THEN SUBSTR(AA.DEP_ETL_OBJ_ID,5)
        |                       ELSE AA.DEP_ETL_OBJ_ID
        |                   END AS DEP_ETL_OBJ_ID ,
        |                   LEVEL leaf ,
        |                         connect_by_root dep_etl_obj_id child_id
        |   FROM ${argBuilder.database}.CCIC_INFA_VIEW_INFO Aa
        |   WHERE REF_ETL_OBJ_ID LIKE 'CCIC_EDW%'
        |     AND REF_ETL_OBJ_ID NOT LIKE 'CCIC_SOURCE%'
        |     AND REF_ETL_OBJ_ID NOT LIKE 'CCIC_FDW%'
        |     AND dep_ETL_OBJ_ID NOT LIKE 'ODS2_FDW%'
        |     START WITH CASE WHEN SUBSTR(AA.DEP_ETL_OBJ_ID,1,4) = 'J_C_' THEN SUBSTR(AA.DEP_ETL_OBJ_ID,5) ELSE AA.DEP_ETL_OBJ_ID END IN ('${argBuilder.retable}') CONNECT BY
        |     PRIOR AA.REF_ETL_OBJ_ID= CASE WHEN SUBSTR(AA.DEP_ETL_OBJ_ID,1,4) = 'J_C_' THEN SUBSTR(AA.DEP_ETL_OBJ_ID,5) ELSE AA.DEP_ETL_OBJ_ID END) ,
        |     TAB1 AS
        |  (SELECT REF_ETL_OBJ_ID ,
        |          DEP_ETL_OBJ_ID ,
        |          MAX(LEAF) AS NUM
        |   FROM TAB
        |   GROUP BY REF_ETL_OBJ_ID,
        |            DEP_ETL_OBJ_ID),
        |     TAB2 AS
        |  (SELECT REF_ETL_OBJ_ID ,
        |          DEP_ETL_OBJ_ID ,
        |          MAX(NUM) OVER(PARTITION BY DEP_ETL_OBJ_ID) AS END_NUM
        |   FROM tab1),
        |     TAB3 AS
        |  (SELECT REF_ETL_OBJ_ID ,
        |          dense_rank() over(
        |                            ORDER BY END_NUM ASC) END_RANK
        |   FROM TAB2),
        |   TAB4 AS(
        |SELECT REF_ETL_OBJ_ID ,
        |       MAX(END_RANK) END_RANK
        |FROM TAB3
        |WHERE REF_ETL_OBJ_ID NOT LIKE '%DD_T_L%'   --过滤掉非车理赔
        |GROUP BY REF_ETL_OBJ_ID
        |UNION ALL
        |SELECT '${argBuilder.retable}' AS REF_ETL_OBJ_ID,
        |        0 AS END_RANK FROM DUAL)
        |SELECT DISTINCT
        |      A.REF_ETL_OBJ_ID
        |      ,A.END_RANK
        |      ,D.PM_VALUE
        |FROM TAB4 A,
        | ${argBuilder.database}.CCIC_INFA_WORKFLOW_INFO C ,
        | ${argBuilder.database}.OPB_TASK T,
        | ${argBuilder.database}.OPB_TASK_VAL_LIST D
        |WHERE A.REF_ETL_OBJ_ID=SUBSTR(C.SESSION_NAME,3)
        |AND C.SESSION_ID=T.TASK_ID
        |AND T.TASK_ID=D.TASK_ID
        |AND T.TASK_NAME NOT LIKE 'C_%'
        |AND T.TASK_NAME <> 'post_session_success_command'
        |AND 1 = ?
        |AND 2 = ?
        |
      """
        .stripMargin

    val data = new JdbcRDD(
      spark.sparkContext
      , createConnection
      , dealDataThings
      , lowerBound = 1
      , upperBound = 2
      , numPartitions = 1
      , extractValues
    )


    val schemaData = data.map(res => ResultData(res._1, res._2, res._3))

    spark.createDataFrame(schemaData).createOrReplaceTempView("CCIC_INFA_VIEW_INFO")
    val resultDataSet = spark.sql(
      """
        |SELECT
        |  REF_ETL_OBJ_ID
        | ,END_RANK
        | ,REGEXP_EXTRACT(PM_VALUE,"(^.*\')(.*)(\'.*$)",2) AS EXEC_COMMAND
        |FROM CCIC_INFA_VIEW_INFO
        |ORDER BY END_RANK DESC
        |
      """.stripMargin)

     showData = resultDataSet.collect()

    val reMakeScript = new RenewDataMakeScript(resultDataSet, argBuilder)
    assert(!reMakeScript.make(),"Make Redata Script occur error.")

    printMessage("Make Redata shell Script Success.")

    spark.stop()
  }


}
