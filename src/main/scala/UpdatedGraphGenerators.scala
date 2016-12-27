/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx.util

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.util._

/** A collection of graph generating functions. */
object UpdatedGraphGenerators extends Logging {


  // Right now it just generates a bunch of edges where
  // the edge data is the weight (default 1)
  val RMATc = 0.15

  /**
    * Generate a graph whose vertex out degree distribution is log normal.
    *
    * The default values for mu and sigma are taken from the Pregel paper:
    *
    * Grzegorz Malewicz, Matthew H. Austern, Aart J.C Bik, James C. Dehnert,
    * Ilan Horn, Naty Leiser, and Grzegorz Czajkowski. 2010.
    * Pregel: a system for large-scale graph processing. SIGMOD '10.
    *
    * If the seed is -1 (default), a random seed is chosen. Otherwise, use
    * the user-specified seed.
    *
    * @param sc          Spark Context
    * @param numVertices number of vertices in generated graph
    * @param numEParts   (optional) number of partitions
    * @param mu          (optional, default: 4.0) mean of out-degree distribution
    * @param sigma       (optional, default: 1.3) standard deviation of out-degree distribution
    * @param seed        (optional, default: -1) seed for RNGs, -1 causes a random seed to be chosen
    * @return Graph object
    */
  def logNormalGraph(
                      sc: SparkContext, numVertices: Int, numEParts: Int = 0, mu: Double = 4.0,
                      sigma: Double = 1.3, seed: Long = -1): Graph[Long, Int] = {

    val evalNumEParts = if (numEParts == 0) sc.defaultParallelism else numEParts

    // Enable deterministic seeding
    val seedRand = if (seed == -1) new Random() else new Random(seed)
    val seed1 = seedRand.nextInt()
    val seed2 = seedRand.nextInt()

    val vertices: RDD[(Long, Long)] = sc.parallelize(0 until numVertices, evalNumEParts).map {
      src => (src.toLong, sampleLogNormal(mu, sigma, numVertices, seed = (seed1 ^ src)))
    }

    val edges = vertices.flatMap { case (src, degree) =>
      generateRandomEdges(src.toInt, degree.toInt, numVertices, seed = (seed2 ^ src))
    }

    Graph(vertices, edges, 0)
  }

  def generateRandomEdges(
                           src: Int, numEdges: Int, maxVertexId: Int, seed: Long = -1): Array[Edge[Int]] = {
    val rand = if (seed == -1) new Random() else new Random(seed)
    val buf = scala.collection.mutable.HashSet.empty[Edge[Int]]
    while (buf.size < numEdges) {
      val r = rand.nextInt(maxVertexId)
      if (src != r) {
        buf += Edge(src, r)
      }
    }
    buf.toArray
  }

  /**
    * Randomly samples from a log normal distribution whose corresponding normal distribution has
    * the given mean and standard deviation. It uses the formula `X = exp(m+s*Z)` where `m`,
    * `s` are the mean, standard deviation of the lognormal distribution and
    * `Z ~ N(0, 1)`. In this function,
    * `m = e^(mu+sigma^2/2)` and `s = sqrt[(e^(sigma^2) - 1)(e^(2*mu+sigma^2))]`.
    *
    * @param mu     the mean of the normal distribution
    * @param sigma  the standard deviation of the normal distribution
    * @param maxVal exclusive upper bound on the value of the sample
    * @param seed   optional seed
    */
  private[spark] def sampleLogNormal(
                                      mu: Double, sigma: Double, maxVal: Int, seed: Long = -1): Long = {
    val rand = if (seed == -1) new Random() else new Random(seed)

    val sigmaSq = sigma * sigma
    val m = math.exp(mu + sigmaSq / 2.0)
    // expm1 is exp(m)-1 with better accuracy for tiny m
    val s = math.sqrt(math.expm1(sigmaSq) * math.exp(2 * mu + sigmaSq))
    // Z ~ N(0, 1)
    var X: Double = maxVal

    while (X >= maxVal) {
      val Z = rand.nextGaussian()
      X = math.exp(mu + sigma * Z)
    }
    math.floor(X).toLong
  }
}

// end of Graph Generators
