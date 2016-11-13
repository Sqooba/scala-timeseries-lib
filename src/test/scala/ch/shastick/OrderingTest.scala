package ch.shastick

import org.scalatest.junit.JUnitSuite
import org.junit.Test
import ch.shastick.immutable.TSEntry

/**
 * Test implicit ordering definition and the (hopefully) efficient
 * merging of ordered collections of time series entries.
 */
class OrderingTest extends JUnitSuite {

	
	@Test def testImplicitOrdering() {
		val unOrd = Seq(TSEntry(11, "B", 10), TSEntry(1, "A", 10))
		
		assert(
		    Seq(TSEntry(1, "A", 10), TSEntry(11, "B", 10)) != unOrd)
		    
		assert(
		    Seq(TSEntry(1, "A", 10), TSEntry(11, "B", 10)) == unOrd.sorted)
		    
	}

  
}