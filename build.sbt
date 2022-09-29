name := "sparkWDSub"
version := "1.0"

scalaVersion := "2.12.16"

val sparkVersion            = "3.2.2"
// val wdsubVersion            = "0.0.16"
val shexsVersion            = "0.2.16"
val pschemaVersion          = "0.1.5"

val catsVersion             = "2.8.0"
val declineVersion          = "2.2.0"
val jacksonVersion          = "2.13.3"
val munitVersion            = "0.7.29"
val munitEffectVersion      = "1.0.7"
val sparkFastTestsVersion   = "1.0.0"
val scallopVersion          = "4.0.4"
val wikidataToolkitVersion  = "0.14.0"

lazy val MUnitFramework = new TestFramework("munit.Framework")

val Java11 = JavaSpec.temurin("11") 

ThisBuild / githubWorkflowJavaVersions := Seq(Java11)


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
  "com.fasterxml.jackson.core"   % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.core"   % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core"   % "jackson-annotations" % jacksonVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.3",

  // WESO components dependencies.
  "es.weso" %% "shex"         % shexsVersion,
  "es.weso" %% "wshex"        % shexsVersion,
  "es.weso" %% "pschema"        % pschemaVersion,

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
  "org.rogach" %% "scallop" % scallopVersion
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

lazy val app = (project in file("."))
  .settings(
    assembly / mainClass := Some("es.weso.wdsub.spark.Main"),
    assembly / assemblyJarName := "sparkwdsub.jar",
  )

assembly / assemblyShadeRules := {
  val shadePackage = "org.globalforestwatch.shaded"
  Seq(
    ShadeRule.rename("cats.kernel.**" -> s"$shadePackage.cats.kernel.@1").inAll
  )
}