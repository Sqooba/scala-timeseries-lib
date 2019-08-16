package io.sqooba.oss.timeseries.windowing

import io.sqooba.oss.timeseries.TimeSeries
import io.sqooba.oss.timeseries.immutable.TSEntry
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.Queue

class WindowSliderSpec extends FlatSpec with Matchers {

  "The WindowBuilder" should "build no windows for and empty series" in {
    WindowSlider.window(Stream(), 1) shouldBe Stream()
  }
  it should "build a single window for a single entry" in {
    val e = TSEntry(10, Unit, 5)
    WindowSlider
      .window(Stream(e), 1)
      .shouldBe(Stream(TSEntry(10, Queue(e), 5)))

    WindowSlider
      .window(Stream(e), 2)
      .shouldBe(Stream(TSEntry(10, Queue(e), 5)))

    WindowSlider
      .window(Stream(e), 10)
      .shouldBe(Stream(TSEntry(10, Queue(e), 5)))

  }
  it should "build correct windows for a discontinuous two-elements time-series" in {
    val biGap = TimeSeries(Seq(TSEntry(1, "A", 1000), TSEntry(2001, "B", 2000)))

    WindowSlider
      .window(biGap.entries.toStream, 1)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(biGap.entries.head), 1001),
          TSEntry(1002, Queue(), 999),
          TSEntry(2001, Queue(biGap.entries.tail.head), 2000)
        )
      )

    WindowSlider
      .window(biGap.entries.toStream, 10)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(biGap.entries.head), 1010),
          TSEntry(1011, Queue(), 990),
          TSEntry(2001, Queue(biGap.entries.tail.head), 2000)
        )
      )
  }
  it should "build correct windows for a continuous two-elements time-series" in {
    val biCont =
      TimeSeries(Seq(TSEntry(1, "A", 1000), TSEntry(1001, "B", 2000)))

    WindowSlider
      .window(biCont.entries.toStream, 1)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(biCont.entries.head), 1000),
          TSEntry(1001, Queue(biCont.entries: _*), 1),
          TSEntry(1002, Queue(biCont.entries.last), 1999)
        )
      )

    WindowSlider
      .window(biCont.entries.toStream, 10)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(biCont.entries.head), 1000),
          TSEntry(1001, biCont.entries, 10),
          TSEntry(1011, Queue(biCont.entries.last), 1990)
        )
      )

    WindowSlider
      .window(biCont.entries.toStream, 1000)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(biCont.entries.head), 1000),
          TSEntry(1001, biCont.entries, 1000),
          TSEntry(2001, Queue(biCont.entries.last), 1000)
        )
      )
  }
  it should "build correct windows for a discontinuous time-series" in {
    val triGap = TimeSeries(
      Seq(TSEntry(1, "A", 100), TSEntry(200, "B", 50), TSEntry(300, "C", 30))
    )

    def e(i: Int): TSEntry[String] = triGap.entries(i)

    WindowSlider
      .window(triGap.entries.toStream, 1)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 101),
          TSEntry(102, Queue(), 98),
          TSEntry(200, Queue(e(1)), 51),
          TSEntry(251, Queue(), 49),
          TSEntry(300, Queue(e(2)), 30)
        )
      )

    WindowSlider
      .window(triGap.entries.toStream, 49)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 149),
          TSEntry(150, Queue(), 50),
          TSEntry(200, Queue(e(1)), 99),
          TSEntry(299, Queue(), 1),
          TSEntry(300, Queue(e(2)), 30)
        )
      )

    WindowSlider
      .window(triGap.entries.toStream, 50)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 150),
          TSEntry(151, Queue(), 49),
          TSEntry(200, Queue(e(1)), 100),
          TSEntry(300, Queue(e(2)), 30)
        )
      )

    WindowSlider
      .window(triGap.entries.toStream, 51)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 151),
          TSEntry(152, Queue(), 48),
          TSEntry(200, Queue(e(1)), 100),
          TSEntry(300, Queue(e(1), e(2)), 1),
          TSEntry(301, Queue(e(2)), 29)
        )
      )

    WindowSlider
      .window(triGap.entries.toStream, 99)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 199),
          TSEntry(200, Queue(e(1)), 100),
          TSEntry(300, Queue(e(1), e(2)), 30)
        )
      )

    WindowSlider
      .window(triGap.entries.toStream, 100)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 199),
          TSEntry(200, Queue(e(0), e(1)), 1),
          TSEntry(201, Queue(e(1)), 99),
          TSEntry(300, Queue(e(1), e(2)), 30)
        )
      )

    WindowSlider
      .window(triGap.entries.toStream, 198)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 199),
          TSEntry(200, Queue(e(0), e(1)), 99),
          TSEntry(299, Queue(e(1)), 1),
          TSEntry(300, Queue(e(1), e(2)), 30)
        )
      )

    WindowSlider
      .window(triGap.entries.toStream, 199)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 199),
          TSEntry(200, Queue(e(0), e(1)), 100),
          TSEntry(300, Queue(e(1), e(2)), 30)
        )
      )

    WindowSlider
      .window(triGap.entries.toStream, 200)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 199),
          TSEntry(200, Queue(e(0), e(1)), 100),
          TSEntry(300, Queue(e(0), e(1), e(2)), 1),
          TSEntry(301, Queue(e(1), e(2)), 29)
        )
      )

  }
  it should "build correct windows for a continuous time-series" in {

    val triCont = TimeSeries(
      Seq(TSEntry(1, "A", 100), TSEntry(101, "B", 49), TSEntry(150, "C", 30))
    )

    def e(i: Int): TSEntry[String] = triCont.entries(i)

    WindowSlider
      .window(triCont.entries.toStream, 1)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 100),
          TSEntry(101, Queue(e(0), e(1)), 1),
          TSEntry(102, Queue(e(1)), 48),
          TSEntry(150, Queue(e(1), e(2)), 1),
          TSEntry(151, Queue(e(2)), 29)
        )
      )

    WindowSlider
      .window(triCont.entries.toStream, 48)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 100),
          TSEntry(101, Queue(e(0), e(1)), 48),
          TSEntry(149, Queue(e(1)), 1),
          TSEntry(150, Queue(e(1), e(2)), 30)
        )
      )

    WindowSlider
      .window(triCont.entries.toStream, 49)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 100),
          TSEntry(101, Queue(e(0), e(1)), 49),
          TSEntry(150, Queue(e(1), e(2)), 30)
        )
      )

    WindowSlider
      .window(triCont.entries.toStream, 50)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 100),
          TSEntry(101, Queue(e(0), e(1)), 49),
          TSEntry(150, Queue(e(0), e(1), e(2)), 1),
          TSEntry(151, Queue(e(1), e(2)), 29)
        )
      )

  }
  it should "build correct windows for a time-series with contiguous and non-contiguous entries" in {

    val argl = TimeSeries(
      Seq(TSEntry(1, "A", 100), TSEntry(101, "B", 49), TSEntry(200, "C", 30))
    )

    def e(i: Int): TSEntry[String] = argl.entries(i)

    WindowSlider
      .window(argl.entries.toStream, 1)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 100),
          TSEntry(101, Queue(e(0), e(1)), 1),
          TSEntry(102, Queue(e(1)), 49),
          TSEntry(151, Queue(), 49),
          TSEntry(200, Queue(e(2)), 30)
        )
      )

    WindowSlider
      .window(argl.entries.toStream, 49)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 100),
          TSEntry(101, Queue(e(0), e(1)), 49),
          TSEntry(150, Queue(e(1)), 49),
          TSEntry(199, Queue(), 1),
          TSEntry(200, Queue(e(2)), 30)
        )
      )

    WindowSlider
      .window(argl.entries.toStream, 50)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 100),
          TSEntry(101, Queue(e(0), e(1)), 50),
          TSEntry(151, Queue(e(1)), 49),
          TSEntry(200, Queue(e(2)), 30)
        )
      )

    WindowSlider
      .window(argl.entries.toStream, 51)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 100),
          TSEntry(101, Queue(e(0), e(1)), 51),
          TSEntry(152, Queue(e(1)), 48),
          TSEntry(200, Queue(e(1), e(2)), 1),
          TSEntry(201, Queue(e(2)), 29)
        )
      )

    WindowSlider
      .window(argl.entries.toStream, 79)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 100),
          TSEntry(101, Queue(e(0), e(1)), 79),
          TSEntry(180, Queue(e(1)), 20),
          TSEntry(200, Queue(e(1), e(2)), 29),
          TSEntry(229, Queue(e(2)), 1)
        )
      )

    WindowSlider
      .window(argl.entries.toStream, 80)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 100),
          TSEntry(101, Queue(e(0), e(1)), 80),
          TSEntry(181, Queue(e(1)), 19),
          TSEntry(200, Queue(e(1), e(2)), 30)
        )
      )

    WindowSlider
      .window(argl.entries.toStream, 98)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 100),
          TSEntry(101, Queue(e(0), e(1)), 98),
          TSEntry(199, Queue(e(1)), 1),
          TSEntry(200, Queue(e(1), e(2)), 30)
        )
      )

    WindowSlider
      .window(argl.entries.toStream, 99)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 100),
          TSEntry(101, Queue(e(0), e(1)), 99),
          TSEntry(200, Queue(e(1), e(2)), 30)
        )
      )

    WindowSlider
      .window(argl.entries.toStream, 100)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 100),
          TSEntry(101, Queue(e(0), e(1)), 99),
          TSEntry(200, Queue(e(0), e(1), e(2)), 1),
          TSEntry(201, Queue(e(1), e(2)), 29)
        )
      )

    WindowSlider
      .window(argl.entries.toStream, 129)
      .shouldBe(
        Stream(
          TSEntry(1, Queue(e(0)), 100),
          TSEntry(101, Queue(e(0), e(1)), 99),
          TSEntry(200, Queue(e(0), e(1), e(2)), 30)
        )
      )

  }
  it should "not accept 0 or negative window sizes" in {
    intercept[IllegalArgumentException](WindowSlider.window(Stream(), -1))
    intercept[IllegalArgumentException](WindowSlider.window(Stream(), 0))
  }
  it should "properly dispatch update calls to the passed aggregator instance and return its result" in {
    val agg = new TestAggregator

    val ts = TimeSeries(
      Seq(TSEntry(1, "A", 100), TSEntry(101, "B", 49), TSEntry(200, "C", 30))
    )

    def e(i: Int): TSEntry[String] = ts.entries(i)

    WindowSlider
      .window(ts.entries.toStream, 1, agg)
      .shouldBe(
        Stream(
          (TSEntry(1, Queue(e(0)), 100), Some(1)),
          (TSEntry(101, Queue(e(0), e(1)), 1), Some(2)),
          (TSEntry(102, Queue(e(1)), 49), Some(3)),
          (TSEntry(151, Queue(), 49), Some(4)),
          (TSEntry(200, Queue(e(2)), 30), Some(5))
        )
      )

    agg.currentValC shouldBe 5
    agg.addAnDropC shouldBe 0
    agg.added shouldBe ts.entries
    agg.addedWindows shouldBe Seq(Queue(), Queue(e(0)), Queue())
    agg.droppedWindows shouldBe Seq(Queue(e(0), e(1)), Queue(e(1)))

  }
  it should "rely on the correct function on the aggregator when entries need to be both added and removed" in {
    val agg = new TestAggregator

    val ts = TimeSeries(
      Seq(TSEntry(1, "A", 100), TSEntry(101, "B", 49), TSEntry(200, "C", 30))
    )

    def e(i: Int): TSEntry[String] = ts.entries(i)

    WindowSlider
      .window(ts.entries.toStream, 99, agg)
      .shouldBe(
        Stream(
          (TSEntry(1, Queue(e(0)), 100), Some(1)),
          (TSEntry(101, Queue(e(0), e(1)), 99), Some(2)),
          (TSEntry(200, Queue(e(1), e(2)), 30), Some(3))
        )
      )

    agg.currentValC shouldBe 3
    agg.addAnDropC shouldBe 1
    agg.added shouldBe ts.entries
    agg.addedWindows shouldBe Seq(Queue(), Queue(e(0)), Queue(e(1)))
    agg.droppedWindows shouldBe Seq(Queue(e(0), e(1)))
  }

  "whatToUpdate" should "refuse empty remaining stream and empty previous window" in {
    intercept[AssertionError](
      WindowSlider.whatToUpdate(Stream(), Queue(), 42, 1)
    )
  }
  it should "correctly indicate to fetch from remaining on an empty window content, independently of window length, and advance to the end of the entry" in {
    WindowSlider
      .whatToUpdate(Stream(TSEntry(10, Unit, 5)), Queue(), 10, 1)
      // The bucket containing the entry should start at 10 and end at 16 -> cursor must advance by 6
      // (buckets are [begin, end[)
      .shouldBe((true, false, 5))

    WindowSlider
      .whatToUpdate(Stream(TSEntry(10, Unit, 5)), Queue(), 10, 10)
      // Window width does not influence where we stop here.
      .shouldBe((true, false, 5))
  }
  it should "correctly indicate to fetch from remaining if the next element in the current window has to remain in the window" in {
    WindowSlider
      .whatToUpdate(
        Stream(TSEntry(10, Unit, 5)),
        Queue(TSEntry(5, Unit, 2)),
        10,
        5
      )
      .shouldBe((true, false, 2))

    WindowSlider
      .whatToUpdate(
        Stream(TSEntry(10, Unit, 5)),
        Queue(TSEntry(5, Unit, 2)),
        10,
        10
      )
      // Should not go further than the original series.
      .shouldBe((true, false, 5))

  }
  it should "return the termination condition when the cursor points to the end of the last added queue entry," +
    "and the remaining entries are empty" in {
    WindowSlider
      .whatToUpdate(Stream(), Queue(TSEntry(10, Unit, 5)), 15, 1)
      .shouldBe((false, false, 0))
  }
  it should "correctly indicate to remove from the current window if the tail of the window is on an end of validity" in {
    WindowSlider
      .whatToUpdate(
        Stream(TSEntry(15, Unit, 5)),
        Queue(TSEntry(5, Unit, 2)),
        8,
        1
      )
      // Expect to remove the entry in the queue and to advance to the next one to be added
      .shouldBe((false, true, 7))
    WindowSlider
      .whatToUpdate(
        Stream(TSEntry(15, Unit, 5)),
        Queue(TSEntry(5, Unit, 2)),
        11,
        4
      )
      // Expect to remove the entry in the queue and to advance to the next one to be added
      .shouldBe((false, true, 4))
    WindowSlider
      .whatToUpdate(
        Stream(TSEntry(15, Unit, 5)),
        Queue(TSEntry(5, Unit, 2), TSEntry(7, Unit, 3)),
        11,
        4
      )
      // Expect to remove the entry in the queue and to advance to the point where the other entry must be removed
      .shouldBe((false, true, 3))
  }

}
