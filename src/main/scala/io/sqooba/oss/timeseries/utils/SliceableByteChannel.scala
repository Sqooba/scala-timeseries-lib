package io.sqooba.oss.timeseries.utils

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel

/** IO abstraction used by this library. This extends the SeekableByteChannel
  * with the ability to return a view of a slice of the channel. Additionally,
  * this trait offers some convenience methods for reading byte arrays and other
  * types.
  */
trait SliceableByteChannel extends SeekableByteChannel {

  /** Not supported by SliceableByteChannel. */
  override def truncate(size: Long): SeekableByteChannel =
    throw new UnsupportedOperationException()

  /** Return a view of this channel that is delimited by the given offsets.
    * @param from the offset of the first byte of the slice (inclusive)
    * @param to the offset of the first byte after the slice (exclusive)
    * @return a view on the current channel
    */
  def slice(from: Long, to: Long): SliceableByteChannel

  /** Read an integer from the end of the channel (big-endian byte order).
    * @param offsetToEnd the number of bytes between the end of the integer bytes
    *                    and the end of the channel
    */
  def readIntFromEnd(offsetToEnd: Long): Int =
    ByteBuffer
      .wrap(readBytesFromEnd(offsetToEnd, Integer.BYTES))
      .getInt

  /** Read a block of bytes from the end of the channel.
    * @note This method copies the bytes it reads. In order to avoid a copy you
    *       can use #slice.
    * @param offsetToEnd the number of bytes between the end of the block bytes
    *                    and the end of the channel
    */
  def readBytesFromEnd(offsetToEnd: Long, length: Int): Array[Byte] =
    readBytes(size() - offsetToEnd - length, length)

  /** Read a block of bytes from the channel.
    * @note This method copies the bytes it reads. In order to avoid a copy you
    *       can use #slice.
    */
  def readBytes(offset: Long, length: Int): Array[Byte] = {
    require(offset + length <= size(), "Cannot read past the end of the channel.")

    position(offset)
    val buffer = ByteBuffer.allocate(length)
    require(read(buffer) == length, "There is not enough data on the channel.")

    buffer.flip()
    buffer.array()
  }
}

object SliceableByteChannel {

  /** Wrap the entire SeekableByteChannel in a sliceable one. */
  def apply(channel: SeekableByteChannel): SliceableByteChannel =
    new DefaultSliceableByteChannel(channel, 0, channel.size())
}

/** Default implementation using a SeeakableByteChannel. For the parameter's
  * specifications see method #slice of the trait.
  */
private class DefaultSliceableByteChannel(
    channel: SeekableByteChannel,
    from: Long,
    to: Long
) extends SliceableByteChannel {
  require(from >= 0, "Start of the slice cannot be negative.")
  require(to >= from, "The end of the slice needs to be larger than its start.")
  require(to <= channel.size(), "The end of the slice cannot be larger than the channel.")

  channel.position(from)

  override def slice(fromOffset: Long, toOffset: Long): SliceableByteChannel =
    new DefaultSliceableByteChannel(channel, from + fromOffset, from + toOffset)

  override def read(dst: ByteBuffer): Int = {
    // if the buffer can read more than is in this slice
    if (dst.remaining() > size - position()) {
      // limit the buffer to the number of bytes at the end of this slice
      dst.limit((size - position()).toInt)
    }

    channel.read(dst)
  }

  override def write(src: ByteBuffer): Int = {
    // if the buffer can write more than is in this slice
    if (src.remaining() > size - position()) {
      // limit the buffer to the number of bytes at the end of this slice
      src.limit((size - position()).toInt)
    }
    channel.write(src)
  }

  override def position(): Long = channel.position() - from

  override def position(newPosition: Long): SliceableByteChannel = {
    if (newPosition + from > to) {
      throw new IndexOutOfBoundsException(s"The new position $newPosition was larger than the size $size.")
    }
    channel.position(from + newPosition)
    this
  }

  override val size: Long = to - from

  override def isOpen: Boolean = channel.isOpen

  override def close(): Unit = channel.close()
}
