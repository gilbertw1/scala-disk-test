name := "scala-disk-test"

version := "0.1"

scalaVersion := "2.9.1"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

seq(com.github.retronym.SbtOneJar.oneJarSettings: _*)

libraryDependencies ++= Seq (
	"net.liftweb" %% "lift-json" % "2.4-M4"
)

crossTarget := new File("/Users/bgilbert/projects/sparc/disk-test/build")