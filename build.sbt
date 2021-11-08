name := "sparkWDSub"
version := "1.0"

scalaVersion := "2.12.10"

val sparkVersion            = "3.1.2"
val wikidataToolkitVersion  = "0.12.1"
val jacksonVersion          = "2.10.0"
// val wdsubVersion            = "0.0.16"
val shexsVersion            = "0.1.97"
val srdfVersion             = "0.1.104"
val utilsVersion            = "0.1.99"
val documentVersion         = "0.0.33"
val catsVersion             = "2.6.1"
val declineVersion          = "2.1.0"
val sparkFastTestsVersion   = "1.0.0"
val munitVersion            = "0.7.27"
val munitEffectVersion      = "1.0.5"

lazy val MUnitFramework = new TestFramework("munit.Framework")

libraryDependencies ++= Seq(

  // Spark dependencies.
  "org.apache.spark" %% "spark-core"      % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"       % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib"     % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-graphx"    % sparkVersion % "provided",

  // Wikidata toolkit dependencies.
  "org.wikidata.wdtk" % "wdtk-dumpfiles"   % wikidataToolkitVersion,
  "org.wikidata.wdtk" % "wdtk-wikibaseapi" % wikidataToolkitVersion,
  "org.wikidata.wdtk" % "wdtk-datamodel"   % wikidataToolkitVersion,
  "org.wikidata.wdtk" % "wdtk-rdf"         % wikidataToolkitVersion,
  "org.wikidata.wdtk" % "wdtk-storage"     % wikidataToolkitVersion,
  "org.wikidata.wdtk" % "wdtk-util"        % wikidataToolkitVersion,

  // Jackson dependencies.
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.2",

  // WESO components dependencies.
  "es.weso" %% "document"     % documentVersion,
  "es.weso" %% "srdf"         % srdfVersion,
  "es.weso" %% "srdfjena"     % srdfVersion,
  "es.weso" %% "srdf4j"       % srdfVersion,
  "es.weso" %% "utils"        % utilsVersion,
  "es.weso" %% "shex"         % shexsVersion,

  // Cats dependencies.
  "org.typelevel" %% "cats-core"    % catsVersion,
  "org.typelevel" %% "cats-kernel"  % catsVersion,

  // Decline dependencies.
  "com.monovore" %% "decline"        % declineVersion,
  "com.monovore" %% "decline-effect" % declineVersion,

  // Testing dependencies
  "com.github.mrpowers"          %% "spark-fast-tests"    % sparkFastTestsVersion % Test,

  // Munit dependencies.
  "org.scalameta" %% "munit"               % munitVersion % Test,
  "org.typelevel" %% "munit-cats-effect-3" % munitEffectVersion % Test,

  // CLI command parsing library.
  "org.rogach" %% "scallop" % "4.0.4"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

lazy val app = (project in file("."))
  .settings(
    assembly / mainClass := Some("es.weso.wqsub.spark.Main"),
    assembly / assemblyJarName := "sparkwdsub.jar",
  )

assembly / assemblyShadeRules := {
  val shadePackage = "org.globalforestwatch.shaded"
  Seq(
    ShadeRule.rename("cats.kernel.**" -> s"$shadePackage.cats.kernel.@1").inAll
  )
}