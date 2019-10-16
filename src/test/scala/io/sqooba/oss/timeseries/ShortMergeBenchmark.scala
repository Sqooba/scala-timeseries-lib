package io.sqooba.oss.timeseries

object ShortMergeBenchmark {

  def main(args: Array[String]): Unit = {
    val (avg1, std1) = benchmarkFunction(iterations = 10) { () =>
      val (ts1, ts2) = createFixture
      TimeSeriesMerger.mergeEntries(ts1.entries)(ts2.entries)(NumericTimeSeries.nonStrictPlus[Int, Int])
    }

    printResults("TimeSeriesMerger.mergeEntries", avg1, std1)
  }

  private def benchmarkFunction(iterations: Int)(f: () => Any): (Double, Double) = {
    def timedFunction: Long = {
      val start = System.nanoTime()
      f()
      System.nanoTime() - start
    }

    val times = Seq.fill(iterations)(timedFunction)

    val avgTime = times.sum / iterations.toDouble
    val std     = Math.sqrt(times.map(t => Math.pow(t - avgTime, 2)).sum / iterations.toDouble)

    (avgTime, std)
  }

  private def createFixture: (TimeSeries[Int], TimeSeries[Int]) = (
    TimeSeriesBenchmark.createTimeSeriesWithSize(100000),
    TimeSeriesBenchmark.createTimeSeriesWithSize(2 * 100000)
  )

  private def printResults(testName: String, avgNanos: Double, stdNanos: Double): Unit = {
    // scalastyle:off println
    println(f"$testName: Average duration ${avgNanos / 1E9}%2.2fs, std ${stdNanos / 1E9}%2.2fs.")
    // scalastyle:on println
  }

}
