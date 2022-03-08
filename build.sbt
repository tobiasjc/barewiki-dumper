lazy val root = project
  .in(file("."))
  .settings(
    name := "barewiki-dumper",
    organization := "searchonmath",
    scalaVersion := "3.1.1",
    version := "1.0",
    libraryDependencies ++= Seq(
      "org.postgresql" % "postgresql" % "42.3.3" % Compile,
      "org.apache.commons" % "commons-compress" % "1.21" % Compile,
      "org.slf4j" % "slf4j-api" % "1.7.36",
      "org.slf4j" % "slf4j-simple" % "1.7.36"
    ),
    assembly / mainClass := Some("org.barewiki.dumper.App"),
    assembly / assemblyJarName := s"${name.value}-${version.value}.jar"
  )
