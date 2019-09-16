package io.sqooba.oss.timeseries.archive

import java.io.{ByteArrayOutputStream, File, FileInputStream, FileOutputStream}

import io.sqooba.oss.timeseries.TimeSeries
import io.sqooba.oss.timeseries.immutable.{NestedTimeSeries, TSEntry}
import io.sqooba.oss.timeseries.thrift.{TBlockType, TSampledBlockType, TTupleBlockType}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.SortedMap
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
    .map(
      entry => TSEntry(entry.timestamp, GorillaBlock.compress(entry.value), entry.validity)
    )

  private val sampledBlocks = buckets
    .map(
      entry =>
        TSEntry(
          entry.timestamp,
          GorillaBlock.compressSampled(entry.value, 100),
          entry.validity
      )
    )

  private def getChannel(blocks: Seq[TSEntry[GorillaBlock]]) = {
    val tempFile = File.createTempFile("ts_binary_format_spec_temp", "")
    GorillaSuperBlock.write(blocks, new FileOutputStream(tempFile))
    (new FileInputStream(tempFile)).getChannel
  }

  "GorillaSuperBlock" should "write metadata correctly to a stream" in {
    val channel = getChannel(blocks)

    val thriftLength = readIntFromEnd(channel, 0)
    val tryMetadata  = GorillaSuperBlock.readMetadata(channel)
    tryMetadata.isSuccess shouldBe true

    val Success((metadata, length)) = tryMetadata

    metadata.version shouldBe GorillaSuperBlock.VERSION_NUMBER
    metadata.blockType shouldBe TBlockType.Tuple(TTupleBlockType())

    GorillaSuperBlock.marshaller
      .decode(
        readBytesFromEnd(channel, Integer.BYTES, thriftLength)
      )
      .get shouldBe metadata

    length shouldBe thriftLength

    GorillaSuperBlock.marshaller
      .decode(
        readBytesFromEnd(channel, Integer.BYTES, thriftLength - 13)
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
    val channel     = getChannel(blocks)
    val (_, length) = GorillaSuperBlock.readMetadata(channel).get

    GorillaSuperBlock.readIndex(channel, length) shouldBe Map(
      1    -> 0,
      77   -> 92,
      1000 -> 184
    )
  }

  it should "correctly compress and decompress the entries" in {
    val channel            = getChannel(blocks)
    val (metadata, length) = GorillaSuperBlock.readMetadata(channel).get
    val indexVector        = GorillaSuperBlock.readIndex(channel, length).toVector

    GorillaSuperBlock
      .readBlock(
        channel,
        indexVector(0)._2,
        (indexVector(1)._2 - indexVector(0)._2).toInt,
        metadata
      )
      .decompress shouldBe buckets.head.value

    GorillaSuperBlock
      .readBlock(
        channel,
        indexVector(1)._2,
        (indexVector(2)._2 - indexVector(1)._2).toInt,
        metadata
      )
      .decompress shouldBe buckets.tail.head.value
  }

  it should "return the same timeseries" in {
    NestedTimeSeries
      .ofGorillaBlocks(
        GorillaSuperBlock.readAll(getChannel(blocks))
      )
      .entries shouldBe entries
  }

  it should "return the same timeseries composed of a single block" in {
    NestedTimeSeries
      .ofGorillaBlocks(
        GorillaSuperBlock.readAll(getChannel(blocks.take(1)))
      )
      .entries shouldBe buckets.head.value
  }

  it should "correctly compress and decompress a sampled block type series" in {
    val channel            = getChannel(sampledBlocks)
    val (metadata, length) = GorillaSuperBlock.readMetadata(channel).get

    metadata.blockType shouldBe TBlockType.Sample(TSampledBlockType(100))

    val indexVector = GorillaSuperBlock.readIndex(channel, length).toVector

    // thread the decompressed through a timeseries such that they get trimmed
    TimeSeries(
      GorillaSuperBlock
        .readBlock(
          channel,
          indexVector(0)._2,
          (indexVector(1)._2 - indexVector(0)._2).toInt,
          metadata
        )
        .decompress
    ).entries shouldBe buckets.head.value

    TimeSeries(
      GorillaSuperBlock
        .readBlock(
          channel,
          indexVector(1)._2,
          (indexVector(2)._2 - indexVector(1)._2).toInt,
          metadata
        )
        .decompress
    ).entries shouldBe buckets.tail.head.value
  }
}
