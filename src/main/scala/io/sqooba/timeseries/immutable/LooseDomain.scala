package io.sqooba.timeseries.immutable

import scala.math.{max, min}

/**
  * This class represents the loose definition domain of a time-series
  *
  * The idea behind a loose domain is to represent the minimum and maximum values
  * where a time-series is defined. Said otherwise, we know that the time-series is
  * not defined outside of its loose domain.
  *
  * @param start The minimum timestamp where the domain is defined (included, by convention in the whole project)
  * @param until The maximum timestamp where the domain is defined (not included, by convention in the whole project)
  */
case class LooseDomain(start: Long, until: Long) {

  assert(start < until, "A LooseDomain can not be empty")

  def contains(p: Long): Boolean = p >= start && p < until

  def union(other: LooseDomain): LooseDomain =
    LooseDomain(min(start, other.start), max(until, other.until))

  def intersect(other: Option[LooseDomain]): Option[LooseDomain] = other match {
    case None => None
    case Some(LooseDomain(otherStart, otherUntil)) =>
      val (nStart, nUntil) = (max(start, otherStart), min(until, otherUntil))

      if (nStart < nUntil) Some(LooseDomain(nStart, nUntil))
      else None
  }

}
