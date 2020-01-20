package io.sqooba.oss.timeseries.archive

import java.io.{ByteArrayOutputStream, File, FileInputStream, FileOutputStream}

import io.sqooba.oss.timeseries.TimeSeries
import io.sqooba.oss.timeseries.bucketing.TimeBucketer
import io.sqooba.oss.timeseries.immutable.{NestedTimeSeries, TSEntry}
import io.sqooba.oss.timeseries.thrift.{TBlockType, TSampledBlockType, TTupleBlockType}
import io.sqooba.oss.timeseries.utils.SliceableByteChannel
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Success

class GorillaSuperBlockSpec extends FlatSpec with Matchers {

  private val entries = Stream(
    TSEntry(1, 200.03d, 49),
    TSEntry(50, 400.03d, 100),
    TSEntry(77, 100.03d, 100),
    TSEntry(200, 0.123456789d, 100)
  )

  private val buckets = TimeBucketer
    .bucketEntries(entries, Stream.from(0, 1000).map(_.toLong), 2)

  private val blocks = buckets
    .map(entry => TSEntry(entry.timestamp, GorillaBlock.compress(entry.value), entry.validity))

  private val sampledBlocks = buckets
    .map(entry =>
      TSEntry(
        entry.timestamp,
        GorillaBlock.compressSampled(entry.value, 100),
        entry.validity
      )
    )

  private def getChannel(blocks: Seq[TSEntry[GorillaBlock]]) = {
    val tempFile = File.createTempFile("ts_binary_format_spec_temp", "")
    GorillaSuperBlock.write(blocks, new FileOutputStream(tempFile))

    SliceableByteChannel(new FileInputStream(tempFile).getChannel)
  }

  "GorillaSuperBlock" should "write metadata correctly to a stream" in {
    val channel = getChannel(blocks)

    val thriftLength = channel.readIntFromEnd(0)
    noException should be thrownBy GorillaSuperBlock(channel).readMetadata

    val (metadata, length) = GorillaSuperBlock(channel).readMetadata

    metadata.version shouldBe GorillaSuperBlock.VERSION_NUMBER
    metadata.blockType shouldBe TBlockType.Tuple(TTupleBlockType())

    GorillaSuperBlock.marshaller
      .decode(
        channel.readBytesFromEnd(Integer.BYTES, thriftLength)
      )
      .get shouldBe metadata

    length shouldBe thriftLength

    GorillaSuperBlock.marshaller
      .decode(
        channel.readBytesFromEnd(Integer.BYTES, thriftLength - 13)
      )
      .isFailure shouldBe true
  }

  it should "compress a single GorillaBlock" in {
    val output = new ByteArrayOutputStream()
    noException should be thrownBy GorillaSuperBlock.write(blocks.take(1), output)
  }

  it should "throw if no buckets are provided" in {
    a[RuntimeException] should be thrownBy
      GorillaSuperBlock.write(Stream.empty, new ByteArrayOutputStream())
  }

  it should "correctly compress and decompress the index" in {
    val block       = GorillaSuperBlock(getChannel(blocks))
    val (_, length) = block.readMetadata

    block.readIndex(length) shouldBe Map(
      1    -> 0,
      77   -> 92,
      1000 -> 184
    )
  }

  it should "correctly compress and decompress the entries" in {
    val superBlock         = GorillaSuperBlock(getChannel(blocks))
    val (metadata, length) = superBlock.readMetadata
    val indexVector        = superBlock.readIndex(length).toVector

    superBlock
      .readBlock(
        metadata,
        indexVector(0)._2,
        (indexVector(1)._2 - indexVector(0)._2).toInt
      )
      .decompress shouldBe buckets.head.value

    superBlock
      .readBlock(
        metadata,
        indexVector(1)._2,
        (indexVector(2)._2 - indexVector(1)._2).toInt
      )
      .decompress shouldBe buckets.tail.head.value
  }

  it should "return the same timeseries" in {
    NestedTimeSeries
      .ofGorillaBlocks(
        GorillaSuperBlock(getChannel(blocks)).readAll
      )
      .entries shouldBe entries
  }

  it should "return the same timeseries composed of a single block" in {
    NestedTimeSeries
      .ofGorillaBlocks(
        GorillaSuperBlock(getChannel(blocks.take(1))).readAll
      )
      .entries shouldBe buckets.head.value
  }

  it should "correctly compress and decompress a sampled block type series" in {
    val superBlock         = GorillaSuperBlock(getChannel(sampledBlocks))
    val (metadata, length) = superBlock.readMetadata

    metadata.blockType shouldBe TBlockType.Sample(TSampledBlockType(100))

    val indexVector = superBlock.readIndex(length).toVector

    // thread the decompressed through a timeseries such that they get trimmed
    TimeSeries(
      superBlock
        .readBlock(
          metadata,
          indexVector(0)._2,
          (indexVector(1)._2 - indexVector(0)._2).toInt
        )
        .decompress
    ).entries shouldBe buckets.head.value

    TimeSeries(
      superBlock
        .readBlock(
          metadata,
          indexVector(1)._2,
          (indexVector(2)._2 - indexVector(1)._2).toInt
        )
        .decompress
    ).entries shouldBe buckets.tail.head.value
  }
}
