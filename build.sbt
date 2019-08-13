organization := "io.sqooba"
scalaVersion := "2.12.8"
name := "scala-timeseries-lib"
description := "Lightweight, functional and exact time-series library for scala"
homepage := Some(url("https://github.com/Sqooba/scala-timeseries-lib"))
licenses := Seq("Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))

crossScalaVersions := Seq("2.13.0-M5", "2.12.8", "2.11.12")

libraryDependencies ++= Seq(
  "com.storm-enroute" %% "scalameter" % "0.17",
  "fi.iki.yak" % "compression-gorilla" % "2.1.1",
  "junit"             % "junit"       % "4.12" % Test,
  "org.scalactic"     %% "scalactic"  % "3.0.7",
  "org.scalatest"     %% "scalatest"  % "3.0.7" % Test
)

coverageHighlighting := true
publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ =>
  false
}
parallelExecution in Test := true

scmInfo := Some(
  ScmInfo(
    url("https://github.com/Sqooba/scala-timeseries-lib"),
    "scm:git@github.com:Sqooba/scala-timeseries-lib.git"
  )
)
developers := List(
  Developer("shastick", "Shastick", "", url("https://github.com/Shastick"))
)
