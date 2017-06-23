package hw4

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object PageRank {
	def main(args: Array[String]) {
		try { 
			    val conf = new SparkConf().setAppName("pagerank").setMaster("local")        
					val sc = new SparkContext(conf)

					// create an instance for BZ2Parser and call the function parse123 of it on each line of input
					// to parse it.
					// RDD input has format:- Array[String], each string contains URL "\t" [outlink-url1, outlink-url2, ..]
					// or "Dummy" which represents non-valied tupples.
					val p: BZ2Parser = new BZ2Parser
					var input = sc.textFile(args(0)).map(l => p.parse123(l)).persist()

					// Dummy is assigned to records which do not match the pattern(URL followed by outlinks, outlinks can be empty)
					//from BZ2parse

					// Remove dummy array items out of input
					input = input.filter(line => line != "dummy")

					// pagecount(Double): is the count of records which match the pattern
					val pagecount = input.count().toDouble

					// initialPageRank(Double): assign inital Pagerank by distributed 1 by all URLs
					val initialPageRank = 1.0/pagecount


					// pagerank:- Array[(String, (Double, Array[String]))] 
					//contains array of (URL, (Pagerank for theURL, array of outlinks))
					// for the initial iteration, initialPageRank is assigned as pagerank for each page
					// on splitiing one string of input array RDD, we get URL, outlinks(seperated by ",")
					// substring(1, line(1).length() - 1 is used to remove [ and ]
					// the key will be the URL, value will be InitialPageRank, outlinks array,
					//which is achieved by splitting the outlinks string on ","
					var pagerank = input.map(line => line.split("\t")).
					               keyBy(line => line(0)).
					               mapValues(line =>  (initialPageRank, line(1).trim().substring(1, line(1).length() - 1).trim().split(",")))



					// iterate 10 times
					for( a <- 1 to 10){

						// Handling dangling nodes:
						// DandlingNodesPRsum: it is the sum of pageranks of all URLs with no out links from the previous iteration.
						// This value will be distributed equally to all node's pagerank
						// line._2._2 is the array of outlinks, if it is empty, we are mapping on _._2._1(pagerank) and getting the sum of it
						var DandlingNodesPRsum = pagerank.filter(line => line._2._2.forall(_.isEmpty)).map(_._2._1).sum()

						// DanglingNodePRdist is the value that will be added to each pages pagerank inorder to distribute it equally
						var DanglingNodePRdist = DandlingNodesPRsum/pagecount

						// paagearank1 will be RDD of Array[(String, Double)] where String is URL(outlink to its parent URL) and 
						// the Double is the new pagerank distribution from its parent link to it
						// for each link which is an outlink of a url in pagerank, maps its url with its pagerank distribution
						var paagerank1 = pagerank.flatMap{
							 l => var PRdist = l._2._1/l._2._2.length
							 l._2._2.map(line => (line.trim(), PRdist))}

						// paagerank2 will be RDD of type Array [(String, Double)] sums the pagerank distributions for the same url
						// sums the value part if the key is the same
						var paagerank2 = paagerank1.reduceByKey((x, y) => x + y).keyBy(l => l._1)mapValues(l => l._2)
						
						// prjoin1: Array[(String, (Double, (Double, Array[String])))] 
						// join based on keys for paagerank2 and pagerank. 
						// first double is the accumilated pagerank distribution for a page(used to caliculate new pagerank)
						// second element is(Double, Array[String]) which is previous pagerank and outlinks of URL respectively
						var prjoin1 = paagerank2.join(pagerank)

					  // reinitializing pagerank: Array[(String, (Double, Array[String]))] 
						// use the prdistribution of prjoin1 to caliculate new pagerank,
						// also add danglingNode's Pagerank distribution to each URL
						// we do not need the old page rank,
						// the outlinks remain the same, so assign the outlinks in the place of outlinks
						pagerank = prjoin1.map(l => l).keyBy(l => l._1).
					  mapValues(l => ((0.15 * 1/ pagecount) + (0.85 * l._2._1) + DanglingNodePRdist, l._2._2._2))
					}// iteration ends

			    // keeping only the URL and its pagerank vlues
					val output = pagerank.map(l => (l._1, l._2._1))

					// sort and get the top 100 URLs and pageranks
					val out = output.sortBy(_._2, false).take(100)
					sc.parallelize(out).repartition(1).sortBy(_._2, false).saveAsTextFile(args(1))

		} catch {
		case _:Throwable =>
		}


	}
}