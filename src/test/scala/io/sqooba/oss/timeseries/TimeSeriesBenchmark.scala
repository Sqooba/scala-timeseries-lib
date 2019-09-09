package io.sqooba.oss.timeseries

import java.lang.Math.floorMod

import io.sqooba.oss.timeseries.immutable.TSEntry
import org.scalameter.api._

import scala.util.Random

object TimeSeriesBenchmark extends Bench.ForkedTime {

  val random: Random.type = Random

  def randomEntryWithTimestamp(timestamp: Long): TSEntry[Int] =
    TSEntry(timestamp, random.nextInt(30), floorMod(random.nextLong(), 10L) + 1)

  def createTimeSeriesWithSize(size: Int): TimeSeries[Int] = {
    val entries = new Array[TSEntry[Int]](size)
    entries(0) = randomEntryWithTimestamp(0)

    for (i <- 1 until size) {
      val newTimestamp = entries(i - 1).definedUntil + floorMod(random.nextLong(), 10)
      entries(i) = randomEntryWithTimestamp(newTimestamp)
    }

    TimeSeries.ofOrderedEntriesSafe(entries.toVector)
  }

  val sizes: Gen[Int] = Gen.range("size")(100000, 1000000, 100000)

  val ts: Gen[(TimeSeries[Int], TimeSeries[Int])] = for {
    size <- sizes
  } yield (createTimeSeriesWithSize(size), createTimeSeriesWithSize(size))

  performance of "TimeSeries" config (
    exec.independentSamples -> 4,
    exec.benchRuns          -> 4
  ) in {
    measure method "merge" in {
      using(ts) in {
        case (ts1, ts2) =>
          ts1.merge[Int, Int] {
            case (Some(v), _) => Some(v)
            case _            => None
          }(ts2)
      }
    }
  }
}
