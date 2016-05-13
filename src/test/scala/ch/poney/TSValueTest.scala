package ch.poney

import org.junit.Test
import org.scalatest.junit.JUnitSuite

class TSValueTest extends JUnitSuite {
  @Test def test() {
    assert(!TSValue("",10).validFor(key=0, atTime= -1))
    assert(TSValue("",10).validFor(key=0, atTime= 0))
    assert(TSValue("",10).validFor(key=0, atTime= 10))
    assert(!TSValue("",10).validFor(key=0, atTime= 11))
  }
}