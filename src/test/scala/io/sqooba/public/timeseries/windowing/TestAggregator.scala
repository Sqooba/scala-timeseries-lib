package io.sqooba.public.timeseries.windowing

import io.sqooba.public.timeseries.immutable.TSEntry

import scala.collection.immutable.Queue
import scala.collection.mutable

class TestAggregator extends ReversibleAggregator[String, Int] {

  var currentValC = 0
  var addAnDropC  = 0

  val added        = mutable.ListBuffer[TSEntry[String]]()
  val addedWindows = mutable.ListBuffer[Queue[TSEntry[String]]]()

  val droppedWindows = mutable.ListBuffer[Queue[TSEntry[String]]]()

  def currentValue: Option[Int] = {
    currentValC += 1
    Some(currentValC)
  }

  def addEntry(e: TSEntry[String], currentWindow: Queue[TSEntry[String]]): Unit = {
    added += e
    addedWindows += currentWindow
  }

  def dropHead(currentWindow: Queue[TSEntry[String]]): Unit =
    droppedWindows += currentWindow

  override def addAndDrop(add: TSEntry[String], currentWindow: Queue[TSEntry[String]]): Unit = {
    addAnDropC += 1
    super.addAndDrop(add, currentWindow)
  }
}
