package io.sqooba.oss.timeseries.validation

import io.sqooba.oss.timeseries.TimeSeries
import io.sqooba.oss.timeseries.immutable.TSEntry
import org.scalatest.{FlatSpec, Matchers}

class TimestampValidatorSpec extends FlatSpec with Matchers {

  "TimestampValidator" should "throw for out of order timestamps" in {
    an[IllegalArgumentException] should be thrownBy TimestampValidator.validate(2, 1)

    an[IllegalArgumentException] should be thrownBy TimestampValidator.validateGorilla(2, 1)

    an[IllegalArgumentException] should be thrownBy
      TimestampValidator.validateGorillaFirst(2, 1)

    an[IllegalArgumentException] should be thrownBy TimestampValidator.validate(Seq(1, 3, 2))
    an[IllegalArgumentException] should be thrownBy TimestampValidator.validateGorilla(Seq(1, 3, 2))
  }

  it should "not throw for correct timestamps" in {
    noException should be thrownBy TimestampValidator.validate(1, 2)

    noException should be thrownBy TimestampValidator.validateGorilla(1, 2)

    noException should be thrownBy
      TimestampValidator.validateGorillaFirst(1, 2)

    noException should be thrownBy TimestampValidator.validate(Seq(1, 2, 3, 1000))
    noException should be thrownBy TimestampValidator.validateGorilla(Seq(1, 200000000, Int.MaxValue))
  }

  it should "throw for singleton or empty seqs" in {
    an[IllegalArgumentException] should be thrownBy TimestampValidator.validate(Seq(1))
    an[IllegalArgumentException] should be thrownBy TimestampValidator.validate(Seq())
    an[IllegalArgumentException] should be thrownBy TimestampValidator.validateGorilla(Seq(1))
    an[IllegalArgumentException] should be thrownBy TimestampValidator.validateGorilla(Seq())
  }

  it should "throw for non-positive timestamps in gorilla" in {
    an[IllegalArgumentException] should be thrownBy TimestampValidator.validateGorilla(0, 1)
    an[IllegalArgumentException] should be thrownBy TimestampValidator.validateGorilla(-10, 1)
    an[IllegalArgumentException] should be thrownBy TimestampValidator.validateGorilla(1, -10)

    an[IllegalArgumentException] should be thrownBy
      TimestampValidator.validateGorillaFirst(0, 1)
    an[IllegalArgumentException] should be thrownBy
      TimestampValidator.validateGorillaFirst(-1, 1)

    an[IllegalArgumentException] should be thrownBy TimestampValidator.validateGorilla(Seq(-1, 0, 2))
  }

  it should "throw for too large gaps in gorilla" in {
    an[IllegalArgumentException] should be thrownBy TimestampValidator.validateGorilla(1, 10000000000L)

    noException should be thrownBy TimestampValidator.validateGorillaFirst(1, 134217726)
    an[IllegalArgumentException] should be thrownBy TimestampValidator.validateGorillaFirst(1, 134217727)
  }

  it should "have the correct gap constant" in {
    TimestampValidator.MaxGapToBlock shouldBe 134217726
  }

  it should "not complain about big gaps in non-Gorilla contexts" in {
    noException should be thrownBy TimeSeries(
      Seq(
        TSEntry(0, 0.0, 1000),
        TSEntry(TimestampValidator.MaxGapToBlock + 1001, 0.1, 1000),
        TSEntry(TimestampValidator.MaxGapToBlock + TimestampValidator.MaxGap + 1002, 0.2, 1000)
      )
    )
  }
}
