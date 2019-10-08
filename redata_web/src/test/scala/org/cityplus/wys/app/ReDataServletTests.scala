package org.cityplus.wys.app

import org.scalatra.test.scalatest._

class ReDataServletTests extends ScalatraFunSuite {

  addServlet(classOf[ReDataServlet], "/*")

  test("GET / on ReDataServlet should return status 200") {
    get("/") {
      status should equal (200)
    }
  }

}
