package org.cityplus.wys.app

import org.scalatra._

class ReDataServlet extends ScalatraServlet {

//  get("/") {
//    <html>
//      <body>
//        <h1>Twirl reporting for duty at @date.toString!</h1>
//      </body>
//    </html>
//  }

  get("/") {
    views.html.date(new java.util.Date)
  }
}
