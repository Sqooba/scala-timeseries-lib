package io.sqooba.oss.timeseries.archive

import fi.iki.yak.ts.compression.gorilla.{GorillaCompressor, LongArrayOutput}
import io.sqooba.oss.timeseries.archive.TSCompressor.GorillaBlock
import io.sqooba.oss.timeseries.immutable.TSEntry

import scala.collection.mutable

/**
  * A wrapper around the Java Gorilla library in the form of a Scala 'mutable.Builder'.
  * The builder takes TSEntries and continually compresses them to two byte arrays that
  * result in a GorillaBlock.
  *
  * @note It does no sanity checks on the TSEntries: you may us the TSEntryFitter for this.
  */
class GorillaBlockBuilder() extends mutable.Builder[TSEntry[Double], GorillaBlock] {

  // These need be vars because the Java implementations don't provide clear() methods.
  private var valueOutput: LongArrayOutput          = _
  private var valueCompressor: GorillaCompressor    = _
  private var validityOutput: LongArrayOutput       = _
  private var validityCompressor: GorillaCompressor = _

  private var resultCalled = false

  clear()

  // Reset the builder to its initial state. The compressors must be set to null,
  // because they rely on the first timestamp which is only available at the
  // first addition of an element.
  override def clear(): Unit = {
    valueOutput = new LongArrayOutput()
    validityOutput = new LongArrayOutput()
    valueCompressor = null
    validityCompressor = null

    resultCalled = false
  }

  override def +=(entry: TSEntry[Double]): this.type = {
    // If this is the first element added, initialise the compressors with its timestamp.
    if (valueCompressor == null || validityCompressor == null) {
      valueCompressor = new GorillaCompressor(entry.timestamp, valueOutput)
      validityCompressor = new GorillaCompressor(entry.timestamp, validityOutput)
    }

    valueCompressor.addValue(entry.timestamp, entry.value)
    validityCompressor.addValue(entry.timestamp, entry.validity)

    this
  }

  override def result(): GorillaBlock = {
    valueCompressor.close()
    validityCompressor.close()

    resultCalled = true

    GorillaBlock(
      longArray2byteArray(valueOutput.getLongArray),
      longArray2byteArray(validityOutput.getLongArray)
    )
  }
}
