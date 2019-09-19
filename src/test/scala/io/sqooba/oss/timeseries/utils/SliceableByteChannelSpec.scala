package io.sqooba.oss.timeseries.utils

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.file.Files

import org.scalatest.{FlatSpec, Matchers}

class SliceableByteChannelSpec extends FlatSpec with Matchers {

  private val ints = Seq.iterate(0, 10)(_ + 1)

  private def getSlice: SliceableByteChannel = {
    val path    = Files.createTempFile("SliceableByteChannelSpec", ".temp")
    val channel = new RandomAccessFile(path.toFile, "rw").getChannel

    channel.write(getBuffer(ints))
    channel.position(0)
    SliceableByteChannel(channel)
  }

  private def getBuffer(ints: Seq[Int]): ByteBuffer = {
    val buffer = ints
      .foldLeft(ByteBuffer.allocate(Integer.BYTES * ints.size))(_.putInt(_))
    buffer.flip()
    buffer
  }

  "SliceableByteChannel" should "read from and write to a file" in {
    val slice = getSlice

    val buffer = ByteBuffer.allocate(ints.size * Integer.BYTES)
    slice.read(buffer) shouldBe buffer.capacity()
    slice.position() shouldBe slice.size

    buffer.flip()
    ints.foreach(i => buffer.getInt shouldBe i)
  }

  it should "return a view of the slice" in {
    val sliced = getSlice.slice(2 * Integer.BYTES, 5 * Integer.BYTES)
    sliced.position() shouldBe 0

    sliced.size shouldBe 3 * Integer.BYTES

    noException should be thrownBy sliced.position(0)
    noException should be thrownBy sliced.position(2 * Integer.BYTES)

    sliced.readIntFromEnd(0) shouldBe 4
  }

  it should "not read too far in the slice" in {
    val sliced = getSlice.slice(2 * Integer.BYTES, 5 * Integer.BYTES)

    sliced.position(sliced.size - 2)
    val buffer = ByteBuffer.allocate(10)
    sliced.read(buffer) shouldBe 2

    buffer.flip()
    buffer.remaining() shouldBe 2
  }

  it should "not write too far in the slice" in {
    val sliced = getSlice.slice(2 * Integer.BYTES, 5 * Integer.BYTES)

    sliced.position(sliced.size - 2)
    val buffer = ByteBuffer.allocate(4).putInt(1234567)
    buffer.flip()
    sliced.write(buffer) shouldBe 2
    buffer.flip()

    sliced.readBytesFromEnd(0, 2) shouldBe
      buffer.array().slice(0, 2)
  }

  it should "read with a much smaller buffer" in {
    val slice = getSlice

    slice.readBytesFromEnd(2 * Integer.BYTES, Integer.BYTES) shouldBe
      ByteBuffer.allocate(Integer.BYTES).putInt(7).array()
  }

  it should "write with a much smaller buffer" in {
    val slice = getSlice

    slice.position(2 * Integer.BYTES)
    val buffer = ByteBuffer.allocate(Integer.BYTES).putInt(123)
    buffer.flip()

    slice.write(buffer) shouldBe buffer.capacity()
    slice.readBytes(0, slice.size.toInt) shouldBe
      Seq(0, 1, 123, 3, 4, 5, 6, 7, 8, 9).flatMap(
        ByteBuffer.allocate(Integer.BYTES).putInt(_).array()
      )
  }

  it should "read bytes in multiple ways" in {
    val slice = getSlice.slice(0, 5 * Integer.BYTES)

    slice.readBytes(slice.size - Integer.BYTES, Integer.BYTES) shouldBe
      slice.readBytesFromEnd(0, Integer.BYTES)

    ByteBuffer
      .wrap(slice.readBytes(slice.size - Integer.BYTES, Integer.BYTES))
      .getInt shouldBe slice.readIntFromEnd(0)

    val buffer = ByteBuffer.allocate(Integer.BYTES)
    slice.position(slice.size - Integer.BYTES)
    slice.read(buffer)
    buffer.flip()

    slice.readBytes(slice.size - Integer.BYTES, Integer.BYTES) shouldBe
      buffer.array()
  }
}
