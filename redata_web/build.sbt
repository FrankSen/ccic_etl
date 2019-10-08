val ScalatraVersion = "2.6.5"

organization := "org.cityplus.wys"

name := "redata_web"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.8"

resolvers += Classpaths.typesafeReleases

libraryDependencies ++= Seq(
  "org.scalatra" %% "scalatra" % ScalatraVersion,
  "org.scalatra" %% "scalatra-scalatest" % ScalatraVersion % "test",
  "ch.qos.logback" % "logback-classic" % "1.2.3" % "runtime",
  "org.eclipse.jetty" % "jetty-webapp" % "9.4.9.v20180320" % "container",
  "javax.servlet" % "javax.servlet-api" % "3.1.0" % "provided",
  "com.typesafe.slick"      %% "slick"             % "3.2.1",
  "com.h2database"          %  "h2"                % "1.4.196",
  "com.mchange"             %  "c3p0"              % "0.9.5.2",
  "org.apache.httpcomponents" % "httpclient"       % "4.5.4",
  "com.alibaba"               % "fastjson"         % "1.2.47"

)

enablePlugins(SbtTwirl)
enablePlugins(ScalatraPlugin)
