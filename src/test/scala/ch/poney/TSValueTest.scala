package ch.poney

import org.junit.Test
import org.scalatest.junit.JUnitSuite
import ch.poney.immutable.TSValue

class TSValueTest extends JUnitSuite {
  
  @Test def test() {
    assert(!TSValue("",10).validAt(valueTime=0, atTime= -1))
    assert(TSValue("",10).validAt(valueTime=0, atTime= 0))
    assert(TSValue("",10).validAt(valueTime=0, atTime= 9))
    assert(!TSValue("",10).validAt(valueTime=0, atTime= 10))
  }
  
  @Test def testToEntry() {
    assert(TSValue("",10).toEntry(0).timestamp == 0)
    assert(TSValue("",10).toEntry(0).validity == 10)
    assert(TSValue("Bob",10).toEntry(0).value == "Bob")
  }
}