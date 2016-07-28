name := "Ontological-Pathfinding"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "1.5.1").
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-logging", "commons-logging").
    exclude("com.esotericsoftware.minlog", "minlog").
    exclude("com.google.guava", "guava").
    exclude("org.apache.hadoop", "hadoop-yarn-api").
    exclude("org.apache.spark", "spark-launcher_2.11").
    exclude("org.spark-project.spark", "unused").
    exclude("org.apache.spark", "spark-network-common_2.11").
    exclude("org.apache.spark", "spark-network-shuffle_2.11").
    exclude("org.apache.spark", "spark-unsafe_2.11")
)

libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0"

resolvers += Resolver.sonatypeRepo("public")
