package io.sqooba.oss.timeseries.zio

import io.sqooba.oss.timeseries.immutable.TSEntry
import zio.console.{getStrLn, putStrLn}
import zio.stream.{Stream, Take, ZSink}
import zio.{ExitCode, Queue, URIO}

object TestApp extends zio.App {

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    logic.exitCode

  private val logic =
    for {
      aes <- AppendableEntryStream.unbounded[String](false)
      _ <-
        aes.addOne(TSEntry(0, "One", 1000)) *>
          aes.addOne(TSEntry(500, "Two", 1000)) *>
          aes.close()
      stream = aes.finalizedEntries
      taken1 <- stream.runCollect
      _ <- putStrLn(s"Taken: ${taken1}")
      taken2 <- stream.runCollect
      _ <- putStrLn(s"Taken: ${taken2}")
    } yield ()
}
