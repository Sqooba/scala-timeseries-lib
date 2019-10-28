package io.sqooba.oss.timeseries.immutable

import scala.math.{max, min}

/**
  * This trait represents the definition of a time series' domain
  *
  * By convention, a `TimeDomain` is a set of timestamps (`Long`) inclusive on its left,
  * and exclusive on its right.
  */
sealed trait TimeDomain {

  /**
    * Checks if the timestamp given is part of the domain
    *
    * @param p The timestamp to check
    * @return `true` if the timestamp is part of the domain, `false` otherwise
    */
  def contains(p: Long): Boolean

  /**
    * Returns the union of two domains without considering potential //holes// in it
    *
    * Using '#' if the element is in the domain and '-' otherwise, a union would be
    * ```
    * ###-----
    * ------##
    * ==========
    * ###---##
    * ```
    *
    * The looseUnion of these two domains is "########".
    *
    * @param other The other domain
    * @return The union of these two domains
    */
  def looseUnion(other: TimeDomain): TimeDomain

  /**
    * Returns the intersection of two domains
    *
    * @param other The other domain
    * @return The intersection of the two domains
    */
  def intersect(other: TimeDomain): TimeDomain

  /**
    * @return The size of the time-domain
    */
  def size: Long

}

object TimeDomain {

  /**
    * A //safe// constructor of `TimeDomain` which returns the correct underlying representation.
    * If the interval is empty, then an `EmptyTimeDomain` is returned.
    *
    * @param start The starting point of the domain (included)
    * @param until The ending point of the domain (not included)
    * @return A `ContiguousTimeDomain` if the domain contains at least one point, an `EmptyDomain` otherwise
    */
  def fromUnsafeInterval(start: Long, until: Long): TimeDomain =
    if (start < until) {
      ContiguousTimeDomain(start, until)
    } else {
      EmptyTimeDomain
    }

}

case class ContiguousTimeDomain(start: Long, until: Long) extends TimeDomain {

  assert(start < until, "A ContiguousTimeDomain can not be empty")

  def contains(p: Long): Boolean = p >= start && p < until

  def looseUnion(other: TimeDomain): TimeDomain = other match {
    case EmptyTimeDomain => this
    case ContiguousTimeDomain(otherStart, otherUntil) =>
      ContiguousTimeDomain(min(start, otherStart), max(until, otherUntil))
  }

  def intersect(other: TimeDomain): TimeDomain = other match {
    case EmptyTimeDomain => EmptyTimeDomain
    case ContiguousTimeDomain(otherStart, otherUntil) =>
      TimeDomain.fromUnsafeInterval(max(start, otherStart), min(until, otherUntil))
  }

  def size: Long = until - start

}

case object EmptyTimeDomain extends TimeDomain {

  def contains(p: Long): Boolean = false

  def looseUnion(other: TimeDomain): TimeDomain = other

  def intersect(other: TimeDomain): TimeDomain = this

  def size: Long = 0

}
