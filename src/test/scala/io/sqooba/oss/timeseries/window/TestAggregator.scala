package io.sqooba.oss.timeseries.window

import io.sqooba.oss.timeseries.immutable.TSEntry

import scala.collection.immutable.Queue
import scala.collection.mutable

class TestAggregator extends TimeUnawareReversibleAggregator[String, Int] {

  var currentValC = 0
  var addAnDropC  = 0

  val added        = mutable.ListBuffer[TSEntry[String]]()
  val addedWindows = mutable.ListBuffer[Queue[TSEntry[String]]]()

  val droppedWindows = mutable.ListBuffer[Queue[TSEntry[String]]]()

  def currentValue: Option[Int] = {
    currentValC += 1
    Some(currentValC)
  }

  override def addEntry(e: TSEntry[String], currentWindow: Queue[TSEntry[String]]): Unit = {
    added += e
    addedWindows += currentWindow
  }

  override def addEntry(e: TSEntry[String]): Unit = addEntry(e, Queue())

  override def dropHead(currentWindow: Queue[TSEntry[String]]): Unit =
    droppedWindows += currentWindow

  def dropEntry(entry: TSEntry[String]): Unit = dropHead(Queue(entry))

  override def addAndDrop(add: TSEntry[String], currentWindow: Queue[TSEntry[String]]): Unit = {
    addAnDropC += 1
    super.addAndDrop(add, currentWindow)
  }
}
