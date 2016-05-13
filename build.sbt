lazy val root = (project in file(".")).
  settings(
    organization := "ch.poneys",
    name := "tslib",
    version := "0.1.0",
    scalaVersion := "2.11.8"
  )

libraryDependencies += "junit" % "junit" % "4.8.1" % "test"
libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % Test
libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
