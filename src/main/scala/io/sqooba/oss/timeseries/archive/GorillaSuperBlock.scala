package io.sqooba.oss.timeseries.archive

import java.io.OutputStream

import io.sqooba.oss.timeseries.immutable.TSEntry
import io.sqooba.oss.timeseries.thrift.{TBlockType, TSampledBlockType, TSuperBlockMetadata}
import io.sqooba.oss.timeseries.utils.{SliceableByteChannel, ThriftMarshaller}
import org.apache.thrift.TException
import org.apache.thrift.protocol.TCompactProtocol

import scala.collection.immutable.SortedMap

// See the doc of the GorillaSuperBlock object for the format specification.

/** The GorillaSuperBlock class lazily wraps a channel that it reads from. This
  * facilitates the reading of the binary format. The block does not perform any
  * reading on the channel unless a method explicitly requires it.
  *
  * @param channel to read from upon method invocation
  */
case class GorillaSuperBlock(channel: SliceableByteChannel) {

  /** Read the metadata of the GorillaSuperBlock. This also checks whether the
    * underlying binary data has the correct version and format.
    *
    * @return the metadata as specified by the thrift definition and its length
    *
    * @throws TException if the metadata cannot be decoded
    */
  def readMetadata: (TSuperBlockMetadata, Int) = {
    val thriftLength = channel.readIntFromEnd(0)

    GorillaSuperBlock.marshaller
      .decode(channel.readBytesFromEnd(Integer.BYTES, thriftLength))
      .map(metadata => {
        require(
          metadata.version == GorillaSuperBlock.VERSION_NUMBER,
          "The binary input's version is not compatible."
        )

        (metadata, thriftLength)
      })
      .get
  }

  /**
    * Read and decompress the index of the GorillaSuperBlock (see format above).
    * @note This assumes that the binary data has the correct version/format.
    *
    * @param metaLength the length of the thrift metadata block in the footer
    * @return the index as a sorted map of (timestamp -> byte-offset) tuples
    */
  def readIndex(metaLength: Int): SortedMap[Long, Long] = {
    val indexBlockEnd = Integer.BYTES + metaLength

    GorillaArray.decompressTimestampTuples(
      channel.readBytesFromEnd(
        indexBlockEnd + Integer.BYTES,
        channel.readIntFromEnd(indexBlockEnd)
      )
    )
  }

  /**
    * Read (but don't decompress) a GorillaBlock from the GorillaSuperBlock.
    * @note This assumes that the binary data has the correct version/format.
    *
    * @param metadata of the SuperBlock that has information needed for deserialization
    * @param offset the start of the block as given by the index in the footer
    * @param length the length of the block in bytes
    * @return a gorilla encoded block of a timeseries
    */
  def readBlock(
      metadata: TSuperBlockMetadata,
      offset: Long,
      length: Int
  ): GorillaBlock = {
    val bytes = channel.readBytes(offset, length)

    metadata.blockType match {
      case TBlockType.Sample(s) => GorillaBlock.fromSampled(bytes, s.sampleRate)
      case _                    => GorillaBlock.fromTupleSerialized(bytes)
    }
  }

  /**
    * Lazily read (but don't decompress) all GorillaBlocks from this
    * GorillaSuperBlock to memory.
    *
    * @note This assumes that the binary data has the correct version/format.
    *
    * @param metadata of the SuperBlock that has information needed for deserialization
    * @param metaLength the length of the thrift metadata block in the footer
    * @return a sequence of gorilla encoded blocks of a timeseries
    */
  def readAllBlocks(metadata: TSuperBlockMetadata, metaLength: Int): Seq[TSEntry[GorillaBlock]] = {
    import scala.collection.compat._
    readIndex(metaLength).toSeq
      .sliding(2)
      .map {
        case (ts, offset) :: (nextTs, nextOffset) :: _ =>
          TSEntry(
            ts,
            readBlock(metadata, offset, (nextOffset - offset).toInt),
            nextTs - ts
          )
      }
      .to(Stream)
  }

  /** Read all of the information of this GorillaSuperBlock and lazily return
    * all contained GorillaBlocks. This uses the more granular methods of this
    * object internally.
    *
    * @return a sequence of gorilla blocks
    *
    * @throws TException if the metadata cannot be decoded
    */
  def readAll: Seq[TSEntry[GorillaBlock]] = (readAllBlocks _).tupled(readMetadata)
}

/** A GorillaSuperBlock is a binary format for storing a long
  * [[io.sqooba.oss.timeseries.TimeSeries]] composed of many GorillaBlocks. It
  * uses a sequential layout with an index to permit quick look-up of individual
  * blocks by timestamp.
  *
  * GorillaSuperBlock Format
  * +-------+-------+ ... +----- -+-------+---+----------+---+
  * | block | block |     | block | Index | L | Thrift   | L |
  * | 1     | 2     |     | N     |       | I | metadata | T |
  * +-------+-------+ ... +-------+-------+---+----------+---+
  *
  * LT: length of Thrift block, LI: length of index
  *
  * The Gorilla blocks are lined up one after another, ordered by timestamp.
  * They all have different lengths that can be recovered from the offsets
  * stored in the index.
  *
  * At the end of the stream there is a footer composed of an additional Gorilla
  * array containing the index and a block of thrift managed metadata. The index
  * maps the start timestamp of each GorillaBlock to its offset from the
  * beginning of the blob (in bytes). The last entry in the index points to the
  * start of the footer.
  *
  * The lengths of the index and of the thrift block are written as 4 byte
  * integers just after the respective blocks. In that manner a user can first
  * read the footer, decode the index and then do fast look-ups for blocks by
  * timestamp.
  */
object GorillaSuperBlock {

  /** Specifies the version number of this binary format */
  val VERSION_NUMBER: Int = 1

  private[archive] val marshaller = ThriftMarshaller
    .forType[TSuperBlockMetadata](new TCompactProtocol.Factory)
    .get

  /** Writes the provided TSEntries of Gorilla blocks to the provided output stream
    * according to the GorillaSuperBlock format.
    *
    * @param buckets well-formed stream of entries containing Gorilla blocks
    * @param output the stream to write to
    */
  def write[B <: GorillaBlock](buckets: Seq[TSEntry[B]], output: OutputStream): Unit =
    buckets.foldLeft(new GorillaSuperBlock.Writer[B](output))(_ addOne _).close()

  /** A 'mutable.Growable' for the iterative writing of a GorillaSuperBlock. */
  class Writer[B <: GorillaBlock](output: OutputStream) extends AutoCloseable {

    // A map of (timestamps -> offset of encoded block in output)
    private var index: SortedMap[Long, Long]          = SortedMap.empty[Long, Long]
    private var metadata: Option[TSuperBlockMetadata] = None

    private var lastLength       = 0L
    private var lastDefinedUntil = 0L
    private var resultCalled     = false

    def addOne(entry: TSEntry[B]): Writer.this.type = {
      val TSEntry(ts, gorillaBlock, _) = entry

      if (metadata.isEmpty) {
        metadata = Some(
          TSuperBlockMetadata(VERSION_NUMBER, blockType(gorillaBlock))
        )
      }

      val blockBytes = gorillaBlock.serialize
      output.write(blockBytes)

      index += (ts -> (currentOffset + lastLength))
      lastLength = blockBytes.length
      lastDefinedUntil = entry.definedUntil

      this
    }

    def clear(): Unit = {
      index = SortedMap.empty[Long, Long]
      lastLength = 0
      lastDefinedUntil = 0
      resultCalled = false
    }

    override def close(): Unit = {
      if (index.isEmpty || metadata.isEmpty) {
        throw new IllegalStateException("Cannot write a GorillaSuperBlock of zero GorillaBlocks.")
      } else if (resultCalled) {
        throw new IllegalStateException("Cannot call result more than once, unless the builder was cleared.")
      }

      resultCalled = true

      // Add a last entry to the index. It points to the end of the last block
      // i.e. serving as an end marker.
      index += (lastDefinedUntil -> (currentOffset + lastLength))

      // write the compressed index and its length
      val compressedIndex = GorillaArray.compressTimestampTuples(index)
      output.write(compressedIndex)
      output.write(int2ByteArray(compressedIndex.length))

      // write the metadata and its length
      val metaBlock = marshaller.encode(metadata.get).get
      output.write(metaBlock)
      output.write(int2ByteArray(metaBlock.length))

      output.close()
    }

    private def currentOffset(): Long = index.lastOption.map(_._2).getOrElse(0)

    private def blockType(gorillaBlock: GorillaBlock): TBlockType =
      gorillaBlock match {
        case SampledGorillaBlock(_, rate) => TBlockType.Sample(TSampledBlockType(rate))
        case _                            => TBlockType.Tuple()
      }
  }
}
