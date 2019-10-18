package io.sqooba.oss.timeseries.stats

import com.codahale.metrics.UniformSnapshot
import org.scalactic.TolerantNumerics
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class UniformDoubleSnapshotSpec extends FlatSpec with Matchers {

  "UniformDoubleSnapshot.computeStats" should "sort the passed array in-place" in {
    val arr = Array(2.0, 1.0)
    UniformDoubleSnapshot.computeStats(arr, 1.0, 2.0)
    arr shouldBe Array(1.0, 2.0)
  }
  it should "compute snapshot data that is the same as the original reservoir sampling classes" in {
    val ints    = Array.fill(ThreadUnsafeDoubleUniformReservoir.DefaultSize)(Random.nextInt)
    val doubles = ints.map(_.toDouble)
    val longs   = ints.map(_.toLong)

    val ourSnap =
      UniformDoubleSnapshot.computeStats(
        doubles,
        doubles.min,
        doubles.max
      )

    val theirs = new UniformSnapshot(longs)

    implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(1.0)

    assert(ourSnap.median === theirs.getValue(.5))
    assert(ourSnap.mean === theirs.getMean)
    assert(ourSnap.stdDev === theirs.getStdDev)
    assert(ourSnap.min.toLong === theirs.getMin)
    assert(ourSnap.max.toLong === theirs.getMax)
  }

}
