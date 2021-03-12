package io.sqooba.oss.timeseries.zio

import io.sqooba.oss.timeseries.immutable.TSEntry
import zio.Chunk
import zio.test.Assertion.equalTo
import zio.test.{assert, suite, testM, DefaultRunnableSpec, ZSpec}
import org.junit.runner.RunWith
import zio.test.junit.ZTestJUnitRunner

@RunWith(classOf[ZTestJUnitRunner])
class AppendableEntryStreamSpec extends DefaultRunnableSpec {

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("AppendableEntryStream")(
      testM("works as expected without compression")(
        for {
          aes <- AppendableEntryStream.unbounded[String](false)
          _ <- aes.addOne(TSEntry(0, "One", 1000)) *>
              aes.addOne(TSEntry(500, "One", 1000)) *>
              aes.close()
          taken <- aes.finalizedEntries.runCollect
        } yield assert(taken)(
          equalTo(Chunk(TSEntry(0, "One", 500), TSEntry(500, "One", 1000)))
        )
      ),
      testM("works as expected with compression")(
        for {
          aes <- AppendableEntryStream.unbounded[String](true)
          _ <- aes.addOne(TSEntry(0, "One", 1000)) *>
              aes.addOne(TSEntry(500, "One", 1000)) *>
              aes.addOne(TSEntry(1000, "Two", 1000)) *>
              aes.close()
          taken <- aes.finalizedEntries.runCollect
        } yield assert(taken)(
          equalTo(Chunk(TSEntry(0, "One", 1000), TSEntry(1000, "Two", 1000)))
        )
      ),
      testM("is not broken by a faulty input")(
        for {
          aes <- AppendableEntryStream.unbounded[String](true)
          _ <- aes.addOne(TSEntry(500, "One", 1000)) *>
              // This is expected to fail -> flip it so if it succeeds we know
              aes.addOne(TSEntry(500, "One", 1000)).flip *>
              aes.addOne(TSEntry(1000, "Two", 1000)) *>
              aes.close()
          taken <- aes.finalizedEntries.runCollect
        } yield assert(taken)(
          equalTo(Chunk(TSEntry(500, "One", 500), TSEntry(1000, "Two", 1000)))
        )
      )
    )
}
