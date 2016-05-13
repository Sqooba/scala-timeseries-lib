package ch.poney

import org.junit.Test
import org.scalatest.junit.JUnitSuite

class TSEntryTest extends JUnitSuite {
  @Test def testTSentry() {
    assert(!TSEntry(0, "", 10).at(-1).isDefined)
    assert(TSEntry(0, "", 10).at(0).isDefined)
    assert(TSEntry(0, "", 10).at(10).isDefined)
    assert(!TSEntry(0, "", 10).at(11).isDefined)
  }
}