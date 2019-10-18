organization := "io.sqooba.oss"
name := "scala-timeseries-lib"
version := "HEAD-SNAPSHOT"
scalaVersion := "2.13.0"
description := "Lightweight, functional and exact time-series library for scala"
homepage := Some(url("https://github.com/Sqooba/scala-timeseries-lib"))
licenses := Seq("Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))

crossScalaVersions := Seq("2.13.0", "2.12.10")

resolvers += Resolver.bintrayRepo("twittercsl", "sbt-plugins")

libraryDependencies ++= Seq(
  "com.storm-enroute"          %% "scalameter"              % "0.19",
  "fi.iki.yak"                 % "compression-gorilla"      % "2.1.1",
  "com.typesafe.scala-logging" %% "scala-logging"           % "3.9.2",
  "org.apache.thrift"          % "libthrift"                % "0.12.0",
  "com.twitter"                %% "scrooge-core"            % "19.9.0",
  "com.chuusai"                %% "shapeless"               % "2.3.3",
  "junit"                      % "junit"                    % "4.12" % Test,
  "org.scalactic"              %% "scalactic"               % "3.0.8",
  "org.scalatest"              %% "scalatest"               % "3.0.8" % Test,
  "org.scala-lang.modules"     %% "scala-collection-compat" % "2.1.2",
  "io.dropwizard.metrics"      % "metrics-core"             % "4.0.0" % Test
)

coverageHighlighting := true
coverageExcludedPackages := "<empty>;io.sqooba.oss.timeseries.thrift.*"

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
  Developer("shastick", "Shastick", "", url("https://github.com/Shastick")),
  Developer("fdevillard", "Florent Devillard", "", url("https://github.com/fdevillard")),
  Developer("nsanglar", "NSanglar", "", url("https://github.com/nsanglar")),
  Developer("yannbolliger", "Yann Bolliger", "", url("https://github.com/yannbolliger"))
)

// For releases, check the Manual Release part on
// http://caryrobbins.com/dev/sbt-publishing/

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) {
    Some("snapshots" at nexus + "content/repositories/snapshots")
  } else {
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }
}
