package io.sqooba.oss.timeseries.archive

import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path}

import io.sqooba.oss.timeseries.bucketing.TimeBucketer
import io.sqooba.oss.timeseries.immutable.{NestedTimeSeries, TSEntry}
import io.sqooba.oss.timeseries.utils.SliceableByteChannel
import org.scalatest.{FlatSpec, Matchers}

class MultiSeriesBlockSpec extends FlatSpec with Matchers {

  private def getTempFile: Path = Files.createTempFile("MultiSeriesBlockSpec", ".tmp")

  private def getChannel(path: Path): FileChannel =
    new RandomAccessFile(path.toFile, "rw").getChannel

  private val entries = Seq(
    TSEntry(1, 200.03d, 49),
    TSEntry(50, 400.03d, 100),
    TSEntry(77, 100.03d, 100),
    TSEntry(200, 0.123456789d, 100),
    TSEntry(300, 0.6789d, 33),
    TSEntry(350, 0.900000000001d, 50)
  )

  private val letterNames = ('a' to 'z').take(entries.size).map(_.toString)

  // each series has one entry more than the previous one, last == entries
  private val series =
    entries.scanLeft(Seq.empty[TSEntry[Double]])(_ :+ _).tail

  private val blocks = series.map { entriesInSeries =>
    TimeBucketer
      .bucketEntries(entriesInSeries.toStream, Stream.from(0, 1000).map(_.toLong), 2)
      .map(
        entry => TSEntry(entry.timestamp, GorillaBlock.compress(entry.value), entry.validity)
      )
  }

  private def superBlockFiles = blocks.map { block =>
    val file = getTempFile
    GorillaSuperBlock.write(block, Files.newOutputStream(file))
    SliceableByteChannel(getChannel(file))
  }

  private def getMultiSeriesChannel(names: Option[Seq[String]] = None) = {
    val outputFile = getTempFile
    MultiSeriesBlock.write(superBlockFiles, names, getChannel(outputFile))
    SliceableByteChannel(getChannel(outputFile))
  }

  "MultiSeriesBlock" should "write the magic number correctly" in {
    val multiChannel = getMultiSeriesChannel()

    multiChannel.readIntFromEnd(0) shouldBe MultiSeriesBlock.STS_MAGIC_NUMBER
    multiChannel.readIntFromEnd(multiChannel.size() - Integer.BYTES) shouldBe
      MultiSeriesBlock.STS_MAGIC_NUMBER
  }

  it should "have a valid thrift footer" in {
    val multiBlock = MultiSeriesBlock(getMultiSeriesChannel())
    val footer     = multiBlock.readFooter

    footer.version shouldBe MultiSeriesBlock.VERSION_NUMBER
    footer.keys shouldBe None
    footer.offsets.size shouldBe entries.size + 1
  }

  it should "be able to index the super blocks by name" in {
    val multiBlock = MultiSeriesBlock(getMultiSeriesChannel(Some(letterNames)))

    val footer = multiBlock.readFooter
    footer.keys.isDefined shouldBe true
    footer.keys.get.toSeq.sortBy(_._2).map(_._1) shouldBe letterNames
  }

  it should "return a GorillaSuperBlock" in {
    val multiBlock = MultiSeriesBlock(getMultiSeriesChannel())
    val footer     = multiBlock.readFooter

    NestedTimeSeries
      .ofGorillaBlocks(
        multiBlock.getSuperBlock(footer, 3).readAll
      )
      .entries shouldBe series(3)
  }

  it should "return a GorillaSuperBlock from a name" in {
    val multiBlock = MultiSeriesBlock(getMultiSeriesChannel(Some(letterNames)))
    val footer     = multiBlock.readFooter

    NestedTimeSeries
      .ofGorillaBlocks(
        multiBlock.getSuperBlock(footer, letterNames(5)).readAll
      )
      .entries shouldBe series(5)
  }
}
