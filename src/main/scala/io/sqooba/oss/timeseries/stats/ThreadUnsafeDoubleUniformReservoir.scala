package io.sqooba.oss.timeseries.stats

import java.util
import java.util.concurrent.ThreadLocalRandom
import UniformDoubleSnapshot._

/**
  * Class inspired by codahale's metrics UniformReservoir and UniformSnapshot,
  * implementing Vitter's R Algorithm.
  *
  * Reimplementing our own version for two main reasons:
  *  - Using doubles instead of longs (no clue if it's actually a good idea: your mileage may vary)
  *  - Don't want to have an external dependency for a reasonably small thing.
  *  - compute all snapshot values in a single function call
  *  - keep the "real" min/max
  *
  * Otherwise, we try to follow the original behaviour.
  *
  * It is named 'ThreadUnsafe' to underline that unlike the original from codahale, it's
  * totally thread unsafe:
  *
  * These classes are (very) mutable and intended for re-use in a single-threaded context.
  *
  * You have been warned.
  */
class ThreadUnsafeDoubleUniformReservoir(reservoirSize: Int) {

  /** Constructor using the default size */
  def this() = this(ThreadUnsafeDoubleUniformReservoir.DefaultSize)

  private var count = 0L
  private var min   = Double.MaxValue
  private var max   = Double.MinValue

  // Our samples reservoir
  private[stats] val values = new Array[Double](reservoirSize)

  def size: Int =
    if (count > values.length) {
      values.length
    } else {
      count.toInt
    }

  def update(value: Double): ThreadUnsafeDoubleUniformReservoir = {
    if (value < min) {
      min = value
    }
    if (value > max) {
      max = value
    }
    if (count < values.length) {
      values(count.toInt) = value
    } else {
      val r = ThreadLocalRandom.current().nextLong(count)
      if (r < values.length) {
        values(r.toInt) = value
      }
    }
    count += 1
    this
  }

  def snapshot(): Stats = {
    if (count == 0) {
      Stats.ZeroStats
    } else if (count >= values.length) {
      // Reservoir was filled: pass it to the snapshot as-is
      computeStats(values, min, max)
    } else {
      // Reservoir not filled: copy the relevant values to their own array
      computeStats(util.Arrays.copyOf(values, count.toInt), min, max)
    }
  }
}

object ThreadUnsafeDoubleUniformReservoir {
  val DefaultSize = 1028
}

case class Stats(min: Double, max: Double, mean: Double, stdDev: Double, median: Double)

object Stats {
  val ZeroStats = Stats(.0, .0, .0, .0, .0)
}

private[stats] object UniformDoubleSnapshot {

  /**
    * @param reservoir the samples for which we want to derive statistics
    * @param min the 'real' observed minimum
    * @param max the 'real' observed maximum
    */
  def computeStats(reservoir: Array[Double], min: Double, max: Double): Stats = {
    // Sort the passed values array in-place: caller should expect this.
    // TODO: this is only required for quantiles: can we make it optional?
    util.Arrays.sort(reservoir)

    val avg = mean(reservoir)

    Stats(
      min,
      max,
      avg,
      stdDev(reservoir, avg),
      // The median is the .5 quantile. Currently not returning more.
      getValue(reservoir, .5)
    )
  }

  def mean(reservoir: Array[Double]): Double =
    if (reservoir.length == 0) {
      .0
    } else {
      var sum = .0
      for (v <- reservoir) sum += v
      sum / reservoir.length
    }

  def stdDev(reservoir: Array[Double], mean: Double): Double =
    if (reservoir.length <= 1) {
      .0
    } else {
      var sum = .0;
      for (v <- reservoir) {
        val diff = v - mean
        sum += diff * diff
      }
      Math.sqrt(sum / (reservoir.length - 1))
    }

  def getValue(reservoir: Array[Double], quantile: Double): Double =
    if (reservoir.length == 0) {
      .0
    } else {
      val pos = quantile * (reservoir.length + 1)
      val idx = pos.toInt
      if (idx < 1) {
        reservoir(0)
      } else if (idx >= reservoir.length) {
        reservoir(reservoir.length - 1)
      } else {
        val lower = reservoir(idx - 1)
        val upper = reservoir(idx)
        lower + (pos - Math.floor(pos)) * (upper - lower)
      }
    }

}
