package org.cityplus.wys.app


import org.scalatra._

import scala.language.postfixOps
import scala.xml.Node
/**
  * @Description
  * @Author franksen
  * @CREATE 2019-09-24-下午11:32
  */

class HttpExample extends ScalatraServlet with FlashMapSupport {
  private def displayPage(title:String, content:Seq[Node]) = Template.page(title, content, url(_))
  private def displayPageOne(title:String, content:Seq[Node]) = TemplateOne.page(title, content)
  get("/date/:year/:month/:day") {
    displayPage("Scalatra: Date Example",
      <ul>
        <li>Year: {params("year")}</li>
        <li>Month: {params("month")}</li>
        <li>Day: {params("day")}</li>
      </ul>
        <pre>Route: /date/:year/:month/:day</pre>
    )
  }

  get("/form") {
    displayPage("Scalatra: Form Post Example",
      <form action={url("/post")} method='POST'>
        Post something: <input name="submission" type='text'/>
        <input type='submit'/>
      </form>
        <pre>Route: /form</pre>
    )
  }
  post("/post") {
    displayPage("Scalatra: Form Post Result",
      <p>You posted: {params("submission")}</p>
        <pre>Route: /post</pre>
    )
  }
  get("/login") {
    (session.get("first"), session.get("last")) match {
      case (Some(first:String), Some(last:String)) =>
        displayPage("Scalatra: Session Example",
          <pre>You have logged in as: {first + "-" + last}</pre>
            <pre>Route: /login</pre>)
      case x =>
        displayPage("Scalatra: Session Example",
          <form action={url("/login")} method='POST'>
            First Name: <input name="first" type='text'/>
            Last Name: <input name="last" type='text'/>
            <input type='submit'/>
          </form>
            <pre>Route: /login</pre>)
    }
  }
  post("/login") {
    (params("first"), params("last")) match {
      case (first:String, last:String) => {
        session("first") = first
        session("last") = last
        displayPage("Scalatra: Session Example",
          <pre>You have just logged in as: {first + " " + last}</pre>
            <pre>Route: /login</pre>)
      }
    }
  }
  get("/logout") {
    session.invalidate
    displayPage("Scalatra: Session Example",
      <pre>You have logged out</pre>
        <pre>Route: /logout</pre>)
  }
  get("/"){
    displayPageOne("REDATA",
      <p>

      </p>
    )
  }
  get("/loginbegin"){
      (session.get("username"), session.get("password")) match {
        case (Some(username: String), Some(password: String)) =>
          displayPageOne("Login-Message",
            <p><pre>You have login in as :{ username + '-' + password}</pre></p>
          )

        case x =>
          displayPageOne("REDATE",
            <p style="color:red">Don`t message match from form.</p>
          )
      }
  }



  post("/loginbegin"){
    (params("username"), params("password")) match {
      case (username: String, password: String) if username == "admin" && password == "admin" =>
        session("username") = username
        session("password") = password

        views.html.index("Index-Page", <p></p>)

        /*displayPageOne("Login-Message",
          <p><pre>You have login in as :{ username + '-' + password}</pre></p>
        )*/
      case _ =>
        displayPageOne("REDATE",
          <label class="control-label"><p style="color:red">Username or password error.</p></label>
        )
    }
  }
  /*get("/") {
    displayPage("Scalatra: Hello World",
      <h2>Hello world!</h2>
        <p>Referer: { (request referrer) map { Text(_) } getOrElse { <i>none</i> }}</p>
        <pre>Route: /</pre>)
  }*/
  get("/flash-map/form") {
    displayPage("Scalatra: Flash Map Example",
      <span>Supports the post-then-redirect pattern</span><br />
        <form method="post">
          <label>Message: <input type="text" name="message" /></label><br />
          <input type="submit" />
        </form>)
  }
  post("/flash-map/form") {
    flash("message") = params.getOrElse("message", "")
    redirect("/flash-map/result")
  }
  get("/flash-map/result") {
    displayPage(
      title = "Scalatra: Flash Map Example",
      content = <span>Message = {flash.getOrElse("message", "")}</span>
    )
  }
}
object Template {
  def page(title:String, content:Seq[Node], url: String => String = identity _, head: Seq[Node] = Nil, scripts: Seq[String] = Seq.empty, defaultScripts: Seq[String] = Seq("/assets/js/jquery.min.js", "/assets/js/bootstrap.min.js")) = {
    <html lang="en">
      <head>
        <title>{ title }</title>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <meta name="description" content="" />
        <meta name="author" content="" />
        <!-- Le styles -->
        <link href="/assets/css/bootstrap.css" rel="stylesheet" />
        <link href="/assets/css/bootstrap-responsive.css" rel="stylesheet" />
        <link href="/assets/css/syntax.css" rel="stylesheet" />
        <link href="/assets/css/scalatra.css" rel="stylesheet" />
        <!-- Le HTML5 shim, for IE6-8 support of HTML5 elements -->
        <!--[if lt IE 9]>
          <script src="http://html5shim.googlecode.com/svn/trunk/html5.js"></script>
        <![endif]-->
        {head}
      </head>
      <body>
        <div class="navbar navbar-inverse navbar-fixed-top">
          <div class="navbar-inner">
            <div class="container">
              <a class="btn btn-navbar" data-toggle="collapse" data-target=".nav-collapse">
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
              </a>
              <a class="brandnew" href="/">Scalatra Examples</a>
              <div class="nav-collapse collapse">
              </div><!--/.nav-collapse -->
            </div>
          </div>
        </div>
        <div class="container">
          <div class="content">
            <div class="page-header">
              <h1>{ title }</h1>
            </div>
            <div class="row">
              <div class="span3">
                <ul class="nav nav-list">
                  <li><a href={url("/cookies-example")}>Cookies example</a></li>
                  <li><a href={url("date/2019/12/26")}>Date example</a></li>
                  <li><a href={url("/upload")}>File upload</a></li>
                  <li><a href={url("/filter-example")}>Filter example</a></li>
                  <li><a href={url("flash-map/form")}>Flash scope</a></li>
                  <li><a href={url("/form")}>Form example</a></li>
                  <li><a href={url("/login")}>Session example(login)</a></li>
                  <li><a href={url("/logout")}>Session example(logout)</a></li>
                  <li><a href="/">Hello world</a></li>
                </ul>
              </div>
              <div class="span9">
                {content}
              </div>
              <hr/>
            </div>
          </div> <!-- /content -->
        </div> <!-- /container -->
        <footer class="vcard" role="contentinfo">
        </footer>
        <!-- Le javascript
          ================================================== -->
        <!-- Placed at the end of the document so the pages load faster -->
        { (defaultScripts ++ scripts) map { pth =>
        <script type="text/javascript" src={pth}></script>
      } }
      </body>
    </html>

      <html lang="en">
        <head>
          <title>{ title }</title>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1.0" />
          <meta name="description" content="" />
          <meta name="author" content="" />
          <!-- Le styles -->
          <link href="/assets/css/bootstrap.css" rel="stylesheet" />
          <link href="/assets/css/bootstrap-responsive.css" rel="stylesheet" />
          <link href="/assets/css/mystyle.css" rel="stylesheet" />
          <!--link href="/assets/css/scalatra.css" rel="stylesheet" /> -->
        </head>
        <body>

          <div class="bg0"></div>
          <div>{content}</div>
        </body>
      </html>
  }
}

object TemplateOne {

  def page(title: String, content: Seq[Node]) ={
      views.html.login(title, content)
  }
}
