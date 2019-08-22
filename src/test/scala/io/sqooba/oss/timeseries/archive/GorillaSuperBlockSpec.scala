package io.sqooba.oss.timeseries.archive

import java.io.{ByteArrayOutputStream, File, FileInputStream, FileOutputStream}
import java.nio.ByteBuffer

import io.sqooba.oss.timeseries.immutable.TSEntry
import org.scalatest.{FlatSpec, Matchers}

class GorillaSuperBlockSpec extends FlatSpec with Matchers {

  private val entries = Stream(
    TSEntry(1, 200.03d, 49),
    TSEntry(50, 400.03d, 100),
    TSEntry(77, 100.03d, 100),
    TSEntry(200, 0.123456789d, 100)
  )

  private val blocks = TimeBucketer
    .bucketEntries(entries, Stream.from(0, 1000).map(_.toLong), 2)
    .map(
      entry => TSEntry(entry.timestamp, GorillaBlock.compress(entry.value), entry.validity)
    )

  private def getChannel = {
    val tempFile = File.createTempFile("ts_binary_format_spec_temp", "")
    GorillaSuperBlock.write(blocks, new FileOutputStream(tempFile))
    (new FileInputStream(tempFile)).getChannel
  }

  "GorillaSuperBlock" should "write two blocks correctly to a stream" in {
    val output = new ByteArrayOutputStream()
    GorillaSuperBlock.write(blocks, output)

    val result = output.toByteArray

    val versionNumber = readIntFromEnd(result, 0)

    versionNumber shouldBe GorillaSuperBlock.VERSION_NUMBER

    val indexLength = readIntFromEnd(result, 4)
    val indexArray  = result.slice(result.length - 8 - indexLength, result.length - 8)
    val indexVector = GorillaArray.decompressTimestampTuples(indexArray).toVector

    val length1 = readIntFromStart(result, 0)
    val offset2 = indexVector(1)._2.toInt
    val entries1 = GorillaBlock.decompress(
      GorillaBlock(
        result.slice(4, 4 + length1),
        result.slice(4 + length1, result.length)
      )
    )
    entries1 shouldBe entries.slice(0, 2)

    val length2 = readIntFromStart(result, offset2)
    val entries2 = GorillaBlock.decompress(
      GorillaBlock(
        result.slice(offset2 + 4, offset2 + 4 + length2),
        result.slice(offset2 + 4 + length2, result.length)
      )
    )
    entries2 shouldBe entries.slice(2, 4)
  }

  it should "throw if no buckets are provided" in {
    a[RuntimeException] should be thrownBy GorillaSuperBlock.write(Stream.empty, new ByteArrayOutputStream())
  }

  it should "correctly compress and decompress the index" in {
    val channel = getChannel

    GorillaSuperBlock.readIndex(channel) shouldBe Map(
      1    -> 0,
      77   -> 92,
      1000 -> 184
    )
  }

  it should "correctly compress and decompress the entries" in {
    val channel     = getChannel
    val indexVector = GorillaSuperBlock.readIndex(channel).toVector

    val block1 = GorillaSuperBlock.readBlock(
      channel,
      indexVector(0)._2,
      (indexVector(1)._2 - indexVector(0)._2).toInt
    )
    GorillaBlock.decompress(
      block1
    ) shouldBe entries.slice(0, 2)

    GorillaBlock.decompress(
      GorillaSuperBlock.readBlock(
        channel,
        indexVector(1)._2,
        (indexVector(2)._2 - indexVector(1)._2).toInt
      )
    ) shouldBe entries.slice(2, 4)
  }

  private def readIntFromEnd(array: Array[Byte], offsetFromEnd: Int): Int =
    ByteBuffer
      .wrap(
        array.slice(
          array.length - offsetFromEnd - java.lang.Integer.BYTES,
          array.length - offsetFromEnd
        )
      )
      .getInt

  private def readIntFromStart(array: Array[Byte], offset: Int): Int =
    ByteBuffer
      .wrap(array.slice(offset, offset + java.lang.Integer.BYTES))
      .getInt
}
