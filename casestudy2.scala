// Databricks notebook source
// MAGIC %md 
// MAGIC # Learning ADB Professionally

// COMMAND ----------

val defaultMoviesUrl = "https://manatarblob.blob.core.windows.net/data/movies.csv"
val defaultRatingsUrl = "adl://manatardls.azuredatalakestore.net/data/ratings.csv"
 
val moviesUrl = dbutils.widgets.text("moviesUrl","")
val ratingsUrl = dbutils.widgets.text("ratingsUrl", "")
 
var inputMoviesUrl = dbutils.widgets.get("moviesUrl")
 
if(inputMoviesUrl == null) {
  inputMoviesUrl = defaultMoviesUrl
}
 
var inputRatingsUrl = dbutils.widgets.get("ratingsUrl")
 
if(inputRatingsUrl == null) {
  inputRatingsUrl = defaultRatingsUrl
}

// COMMAND ----------

package com.microsoft.analytics.utils 

import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction
 
object MovieUtils {
def loadMovieNames(fileName: String): Map[Int, String] = {
  if(fileName == null || fileName == "") {
    throw new Exception("Invalid File / Reference URL Specified!");
  }
 
  implicit val codec = Codec("UTF-8")
 
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
 
  val lines = Source.fromURL(fileName).getLines
 
  lines.drop(1)
 
  var movieNames: Map[Int, String] = Map()
 
  for(line <- lines) {
    val records = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
    val movieId = records(0).toInt
    val movieName = records(1)
 
    movieNames += (movieId -> movieName)
  }
 
  movieNames
}
}

// COMMAND ----------

import com.microsoft.analytics.utils._

val broadcastedMovies = sc.broadcast(() => {MovieUtils.loadMovieNames(inputMoviesUrl)})

// COMMAND ----------

spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", "966deb09-4dc2-4980-94b7-cc556a271e9c")
spark.conf.set("dfs.adls.oauth2.credential", "ZF924orh4INqf5vjGZMXJ/9E+s2WYlx85UPzwXPxs6o=")
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.access.token.provider.type", spark.conf.get("dfs.adls.oauth2.access.token.provider.type"))
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.client.id", spark.conf.get("dfs.adls.oauth2.client.id"))

spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.credential", spark.conf.get("dfs.adls.oauth2.credential"))

spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.refresh.url", spark.conf.get("dfs.adls.oauth2.refresh.url"))

val ratingsData = sc.textFile(inputRatingsUrl)
val originalData = ratingsData.mapPartitionsWithIndex((index, iterator) => {
if(index == 0) iterator.drop(1)

 else iterator
})
val mappedData = originalData.map(line => { val splitted = line.split(",")

(splitted(1).toInt, 1)
})
val reducedData = mappedData.reduceByKey((x, y) => (x + y))
val result = reducedData.sortBy(_._2).collect
val finalOutput = result.reverse.take(10)
val mappedFinalOuptut = finalOutput.map(record => (broadcastedMovies.value()(record._1), record._2))


// COMMAND ----------

case class MovieResult (movieName: String, noOfHits: Int)

val movieResults = mappedFinalOuptut.map(result => MovieResult(result._1, result._2))
val moviesRDD = sc.parallelize(movieResults)
val dataFrame = spark.sqlContext.createDataFrame(moviesRDD)

display(dataFrame)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ![Azure Data Bricks](https://azure.microsoft.com/svghandler/databricks?width=600&height=315)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC \\(c = \\pm\\sqrt{a^2 + b^2} \\)
// MAGIC 
// MAGIC \\( f(\beta)= -Y_t^T X_t \beta + \sum log( 1+{e}^{X_t\bullet\beta}) + \frac{1}{2}\delta^t S_t^{-1}\delta\\)
// MAGIC 
// MAGIC where \\(\delta=(\beta - \mu_{t-1})\\)
// MAGIC 
// MAGIC $$\sum_{i=0}^n i^2 = \frac{(n^2+n)(2n+1)}{6}$$

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <a href="$./anothernotebook">Detailed Customer Review Reports</a>

// COMMAND ----------


displayHTML("""<svg width="100" height="100">
   <circle cx="50" cy="50" r="40" stroke="green" stroke-width="4" fill="yellow" />
   Sorry, your browser does not support inline SVG.
</svg>""")


// COMMAND ----------

val colorsRDD = sc.parallelize(Array((197,27,125), (222,119,174), (241,182,218), (253,244,239), (247,247,247), (230,245,208), (184,225,134), (127,188,65), (77,146,33)))
val colors = colorsRDD.collect()


// COMMAND ----------

displayHTML(s"""
<!DOCTYPE html>
<meta charset="utf-8">
<style>

path {
  fill: yellow;
  stroke: #000;
}

circle {
  fill: #fff;
  stroke: #000;
  pointer-events: none;
}

.PiYG .q0-9{fill:rgb${colors(0)}}
.PiYG .q1-9{fill:rgb${colors(1)}}
.PiYG .q2-9{fill:rgb${colors(2)}}
.PiYG .q3-9{fill:rgb${colors(3)}}
.PiYG .q4-9{fill:rgb${colors(4)}}
.PiYG .q5-9{fill:rgb${colors(5)}}
.PiYG .q6-9{fill:rgb${colors(6)}}
.PiYG .q7-9{fill:rgb${colors(7)}}
.PiYG .q8-9{fill:rgb${colors(8)}}

</style>
<body>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.6/d3.min.js"></script>
<script>

var width = 960,
    height = 500;

var vertices = d3.range(100).map(function(d) {
  return [Math.random() * width, Math.random() * height];
});

var svg = d3.select("body").append("svg")
    .attr("width", width)
    .attr("height", height)
    .attr("class", "PiYG")
    .on("mousemove", function() { vertices[0] = d3.mouse(this); redraw(); });

var path = svg.append("g").selectAll("path");

svg.selectAll("circle")
    .data(vertices.slice(1))
  .enter().append("circle")
    .attr("transform", function(d) { return "translate(" + d + ")"; })
    .attr("r", 2);

redraw();

function redraw() {
  path = path.data(d3.geom.delaunay(vertices).map(function(d) { return "M" + d.join("L") + "Z"; }), String);
  path.exit().remove();
  path.enter().append("path").attr("class", function(d, i) { return "q" + (i % 9) + "-9"; }).attr("d", String);
}

</script>
  """)