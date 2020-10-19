package io.sqooba.oss.timeseries.zio

import io.sqooba.oss.timeseries.immutable.TSEntry
import zio.test.Assertion.{anything, equalTo, isSubtype}
import zio.test.{assert, suite, testM, DefaultRunnableSpec, ZSpec}

object ZEntryFitterSpec extends DefaultRunnableSpec {

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("ZEntryFitter")(
      testM("properly trims and compresses if so specified")(
        for {
          fitter <- ZEntryFitter.init[String](true)
          e1     <- fitter.addAndFitLast(TSEntry(0, "a", 10))
          e2     <- fitter.addAndFitLast(TSEntry(5, "a", 10))
          e3     <- fitter.addAndFitLast(TSEntry(12, "b", 10))
          e4     <- fitter.addAndFitLast(TSEntry(23, "b", 10))
          last   <- fitter.lastEntry
        } yield assert(e1)(equalTo(None)) &&
          assert(e2)(equalTo(None)) &&
          assert(e3)(equalTo(Some(TSEntry(0, "a", 12)))) &&
          assert(e4)(equalTo(Some(TSEntry(12, "b", 10)))) &&
          assert(last)(equalTo(Some(TSEntry(23, "b", 10))))
      ),
      testM("properly trims without compressing if so specified")(
        for {
          fitter <- ZEntryFitter.init[String](false)
          e1     <- fitter.addAndFitLast(TSEntry(0, "a", 10))
          e2     <- fitter.addAndFitLast(TSEntry(5, "a", 10))
          e3     <- fitter.addAndFitLast(TSEntry(12, "b", 10))
          e4     <- fitter.addAndFitLast(TSEntry(23, "b", 10))
          last   <- fitter.lastEntry
        } yield assert(e1)(equalTo(None)) &&
          assert(e2)(equalTo(Some(TSEntry(0, "a", 5)))) &&
          assert(e3)(equalTo(Some(TSEntry(5, "a", 7)))) &&
          assert(e4)(equalTo(Some(TSEntry(12, "b", 10)))) &&
          assert(last)(equalTo(Some(TSEntry(23, "b", 10))))
      ),
      testM("properly fails if the appended entry is before the previous one")(
        for {
          fitter <- ZEntryFitter.init[String](false)
          e1     <- fitter.addAndFitLast(TSEntry(10, "a", 10))
          fail   <- fitter.addAndFitLast(TSEntry(9, "b", 10)).flip
          e2     <- fitter.addAndFitLast(TSEntry(11, "b", 10))
        } yield assert(e1)(equalTo(None)) && assert(fail)(
            isSubtype[IllegalArgumentException](anything)
          ) &&
          assert(e2)(equalTo(Some(TSEntry(10, "a", 1))))
      ),
      testM(
        "properly fails if the appended entry has the same timestamp as the previous one"
      )(
        for {
          fitter <- ZEntryFitter.init[String](false)
          e1     <- fitter.addAndFitLast(TSEntry(10, "a", 10))
          fail   <- fitter.addAndFitLast(TSEntry(10, "b", 10)).flip
          e2     <- fitter.addAndFitLast(TSEntry(11, "b", 10))
        } yield assert(e1)(equalTo(None)) && assert(fail)(
            isSubtype[IllegalArgumentException](anything)
          ) &&
          assert(e2)(equalTo(Some(TSEntry(10, "a", 1))))
      )
    )
}
