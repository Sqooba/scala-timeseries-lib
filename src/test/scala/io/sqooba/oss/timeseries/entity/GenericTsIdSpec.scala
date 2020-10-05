package io.sqooba.oss.timeseries.entity

import io.sqooba.oss.timeseries.labels.TsLabel
import org.scalatest.FlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Failure, Success}

class GenericTsIdSpec extends FlatSpec with Matchers {

  implicit val parser: EntityParser = fruitParser

  "TsId.from" should "be parsable from a string" in {
    GenericTsId.from("test", "ba_1") should be(Success(GenericTsId(BananaId(1L), TsLabel("test"))))

    GenericTsId.from("test_2", "ch_2") should be(Success(GenericTsId(CherryId(2L), TsLabel("test_2"))))
  }

  it should "not be parsable if prefix is unknown" in {
    GenericTsId.from("test_2", "does_not_exist_2") shouldBe a[Failure[_]]
  }

  it should "not be parsable if id is malformed" in {
    GenericTsId.from("test_2", "t_malformed_id") shouldBe a[Failure[_]]
  }
}
