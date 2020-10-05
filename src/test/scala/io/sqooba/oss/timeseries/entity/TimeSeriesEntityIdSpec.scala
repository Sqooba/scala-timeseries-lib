package io.sqooba.oss.timeseries.entity

import io.sqooba.oss.timeseries.labels.TsLabel
import org.scalatest.FlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Failure

class TimeSeriesEntityIdSpec extends FlatSpec with Matchers {

  implicit val parser: EntityParser = fruitParser

  "TimeSeriesEntity" should "return the correct TSDB identification string" in {
    BananaId(666).buildTimeseriesID(TsLabel("bob")).id should be("bob-ba_666")
  }

  "TimeSeriesEntity.from" should "be able to parse a fruit id" in {
    TimeSeriesEntityId.from("ch", 111).get shouldBe CherryId(111L)
  }

  it should "be built from a prefix and an id" in {
    TimeSeriesEntityId.from("ap", 2).get shouldBe AppleId(2L)
  }

  it should "not be created from unknown prefix" in {
    ('A' to 'z')
      .filter(character => character != 'w' && character != 't')
      .foreach { character =>
        TimeSeriesEntityId.from(character.toString, 2) shouldBe a[Failure[_]]
      }
  }

}
