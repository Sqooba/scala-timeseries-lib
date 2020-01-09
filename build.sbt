name := "scala-timeseries-lib"
version := "HEAD-SNAPSHOT"
description := "Lightweight, functional and exact time series library for scala"
homepage := Some(url("https://github.com/Sqooba/scala-timeseries-lib"))
licenses := Seq("Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))

organization := "io.sqooba.oss"
organizationName := "Sqooba"
organizationHomepage := Some(url("https://sqooba.io"))

scalaVersion := "2.13.0"
crossScalaVersions := Seq("2.13.0", "2.12.10")

resolvers += Resolver.bintrayRepo("twittercsl", "sbt-plugins")

libraryDependencies ++= Seq(
  "com.storm-enroute"          %% "scalameter"              % "0.19",
  "fi.iki.yak"                 % "compression-gorilla"      % "2.1.1",
  "com.typesafe.scala-logging" %% "scala-logging"           % "3.9.2",
  "org.apache.thrift"          % "libthrift"                % "0.12.0",
  "com.twitter"                %% "scrooge-core"            % "19.10.0",
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

// SBT Microsite settings
enablePlugins(MicrositesPlugin)
enablePlugins(SiteScaladocPlugin)

micrositeHomepage := "https://sqooba.github.io/scala-timeseries-lib/"
micrositeUrl := "https://sqooba.github.io"
micrositeBaseUrl := "scala-timeseries-lib"
micrositeCompilingDocsTool := WithMdoc
micrositeGithubOwner := "Sqooba"
micrositeGithubRepo := "scala-timeseries-lib"
micrositeDescription := "A lightweight, functional time series library for Scala"
micrositeDocumentationUrl := "latest/api/io/sqooba/oss/timeseries"
micrositePalette := Map(
  "brand-primary"   -> "#00C0F3",
  "brand-secondary" -> "#207692",
  "brand-tertiary"  -> "#0f3e4d",
  "gray-dark"       -> "#49494B",
  "gray"            -> "#69696A",
  "gray-light"      -> "#E5E5E6",
  "gray-lighter"    -> "#F4F3F4",
  "white-color"     -> "#FFFFFF"
)
micrositeImgDirectory := (resourceDirectory in Compile).value / "docs" / "img"

developers := List(
  Developer("shastick", "Shastick", "", url("https://github.com/Shastick")),
  Developer("fdevillard", "Florent Devillard", "", url("https://github.com/fdevillard")),
  Developer("nsanglar", "NSanglar", "", url("https://github.com/nsanglar")),
  Developer("yannbolliger", "Yann Bolliger", "", url("https://github.com/yannbolliger"))
)
