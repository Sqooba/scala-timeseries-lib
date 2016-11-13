package ch.shastick.immutable

import ch.shastick.TimeSeries

case class TSEntry[T]
    (timestamp: Long,           
     value: T,           
     validity: Long) 
     extends TimeSeries[T] {
  
  if(validity <= 0) throw new IllegalArgumentException("Validity must be strictly positive")
 
  def at(t: Long): Option[T] =
    if (timestamp <= t && t < timestamp + validity)
      Some(value)
    else 
      None

  def size(): Int = 1
  
  /** Convert this entry to a value */
  val toVal = TSValue(value, validity)
  
  /** Convert this entry to a time->TSVal tuple to be added to a map */
  val toMapTuple = (timestamp -> toVal)
  
  /** Shorten this entry's validity if it exceed 'at'. No effect otherwise.
   *  
   *  If the entry's timestamp is after 'at', the entry remains unchanged.
   */
  def trimRight(at: Long) = 
    if (at <= timestamp) // Trim before or exactly on value start: result is empty.
      EmptyTimeSeries()
    else // Entry needs to have its validity adapted.
      trimEntryRight(at)
      
  /** Similar to trimLeft, but returns a TSEntry instead of a time series and throws 
   *  if 'at' is before the entry's timestamp. */
  def trimEntryRight(at: Long) =
    if (at <= timestamp) // Trim before or exactly on value start: result is empty.
      throw new IllegalArgumentException(
          s"Attempting to trim right at $at before entry's domain has started ($timestamp)")
    else if (at >= definedUntil) // No effect, return this instance
      this
    else // Entry needs to have its validity adapted.
      TSEntry(timestamp, value, at - timestamp)
  
  /** Move this entry's timestamp to 'at' and shorten the validity accordingly,
   *  if this entry is defined at 'at'. */
  def trimLeft(at: Long) =
    if(at >= definedUntil) // Nothing left from the value on the right side of the trim 
      EmptyTimeSeries()
    else 
      trimEntryLeft(at)
  
  /** Similar to trimLeft, but returns a TSEntry instead of a time series and throws 
   *  if 'at' exceeds the entry's definition. */
  def trimEntryLeft(at: Long) =
    if(at >= definedUntil) // Nothing left from the value on the right side of the trim 
      throw new IllegalArgumentException(
          s"Attempting to trim left at $at after entry's domain has ended ($definedUntil)")
    else if (at <= timestamp) // Trim before or exactly on value start, right side remains unchanged
      this
    else // Entry needs to have its timestamp and validity adapted.
      TSEntry(at, value, definedUntil - at)
      
  /** Equivalent to calling trimEntryLeft(l).trimEntryRight(r)
   *  without the intermediary step. */
  def trimEntryLeftNRight(l: Long, r: Long) =
      if(l >= definedUntil)
        throw new IllegalArgumentException(
          s"Attempting to trim left at $l after entry's domain has ended ($definedUntil)")
      else if(r <= timestamp)
        throw new IllegalArgumentException(
          s"Attempting to trim right at $r before entry's domain has started ($timestamp)")
      else if(l >= r)
        throw new IllegalArgumentException(
            s"Left time must be strictly lower than right time. Was: $l and $r")
      else if(l <= timestamp && r >= definedUntil)
        this
      else {
        val start = Math.max(timestamp, l)
        TSEntry(start, value, Math.min(definedUntil, r) - start)
      }      
  
  def defined(at: Long): Boolean = at >= timestamp && at < definedUntil
  
  /** the last moment where this entry is valid, non-inclusive */
  def definedUntil(): Long = timestamp + validity
  
  /** return true if this and the other entry have an overlapping domain of definition.
   *  False if the domains are only contiguous.*/
  def overlaps[O](other: TSEntry[O]) : Boolean = 
    this.timestamp < other.definedUntil && this.definedUntil > other.timestamp
    
  def toLeftEntry[O]: TSEntry[Either[T, O]] = TSEntry(timestamp, Left[T,O](value), validity)
  
  def toRightEntry[O]: TSEntry[Either[O, T]] = TSEntry(timestamp, Right[O,T](value), validity)
  
  /** Map value contained in this timeseries using the passed function */
  def map[O](f: T => O) = TSEntry(timestamp, f(value), validity)

  def entries: Seq[TSEntry[T]] = Seq(this)
  
  /** Append the other entry to this one.
   *  Any part of this entry that is defined for t > other.timestamp will be overwritten,
   *  either by 'other' or by nothing if 'others's validity does not reach t.
   *  
   *  Ie, if 'other' has a timestamp before this value, only 'other' is returned. */
  def appendEntry(other: TSEntry[T]): Seq[TSEntry[T]] = 
    if(other.timestamp <= timestamp)
      Seq(other)
    else
      Seq(this.trimEntryRight(other.timestamp), other)
  
  /** Prepend the other entry to this one.
   *  Any part of this entry that is defined at t < other.definedUntil will be overwritten by the 
   *  other entry, or not be defined if t < other.timestamp */
  def prependEntry(other: TSEntry[T]): Seq[TSEntry[T]] = 
    if(other.timestamp >= definedUntil) // Complete overwrite, return the other.
      Seq(other)
    else if (other.definedUntil < definedUntil) // Something from this entry to be kept after the prepended one
      Seq(other, this.trimEntryLeft(other.definedUntil))
    else //the prepended entry completely overwrites the present one.
      Seq(other) 

  def head: TSEntry[T] = this

  def headOption: Option[TSEntry[T]] = Some(this)

  def last: TSEntry[T] = this

  def lastOption: Option[TSEntry[T]] = Some(this)

  def append(other: TimeSeries[T]): TimeSeries[T] = 
    other.headOption match {
      case None => // Other TS is empty: no effect 
        this
      // TODO: find out how to get the nice +: syntax working here :)
      case Some(tse) if tse.timestamp > timestamp => 
        other match { 
          // Check if the other TS is also a TSEntry, otherwise we might get into infinite recursion.
          case o: TSEntry[T] => VectorTimeSeries.ofEntries(Seq(this.trimEntryRight(tse.timestamp), o)) 
          case _ => other.prepend(this.trimEntryRight(tse.timestamp))
        }
      case _ => // Hides the current entry completely: just return the other.
        other
    }

  def prepend(other: TimeSeries[T]): TimeSeries[T] = 
    other.lastOption match {
      case None => // Other TS is empty: no effect 
        this
      case Some(tse) if tse.definedUntil() < this.definedUntil() => // other TS overlaps partially  
        other match { 
          // Check if the other TS is also a TSEntry, otherwise we might get into infinite recursion.
          //TODO: find out how to get the nice :+ syntax working here :)
          case o: TSEntry[T] =>  
            VectorTimeSeries.ofEntries(Seq(o, this.trimEntryLeft(tse.definedUntil())))
          case _ => 
            other.append(this.trimEntryLeft(tse.definedUntil()))
        }
      case _ => // Hides the current entry completely: just return the other. 
        other
  }
  
      
}

object TSEntry {
  
  /** Define an implicit ordering for TSEntries of any type */ 
  implicit def orderByTs[T]: Ordering[TSEntry[T]] =
    Ordering.by(ts => ts.timestamp)
    
  /** Build a TSEntry from a tuple containing the a TSValue and the time at which it starts.*/
  def apply[T](tValTup: (Long, TSValue[T])): TSEntry[T] = 
    TSEntry(tValTup._1, tValTup._2.value, tValTup._2.validity)
    
  def apply[T](tup: (Long, T, Long)): TSEntry[T] =
    TSEntry(tup._1, tup._2, tup._3)
    
  /** Merge two overlapping TSEntries and return the result as an
   *  ordered sequence of TSEntries. 
   *    
   *  This method returns a Seq containing one to three TSEntries defining a timeseries valid from
   *  first.timestamp to max(first.validUntil, second.validUntil).
   *    - one entry if first and second share the exact same domain
   *    - two entries if first and second share one bound of their domain, 
   *    - three entries if the domains overlap without sharing a bound
   *    
   *  If the passed merge operator is commutative, then the 'merge' function is commutative as well.
   *  (merge(op)(E_a,E_b) == merge(op)(E_b,E_a) only if op(a,b) == op(b,a))
   */
  protected def mergeOverlapping[A,B,R]
    (a: TSEntry[A], b: TSEntry[B])
    (op: (Option[A], Option[B]) => Option[R])
    : Seq[TSEntry[R]] =
      {    
        // Handle first 'partial' definition
        (Math.min(a.timestamp, b.timestamp), Math.max(a.timestamp, b.timestamp)) match {
        case (from, to) if (from != to) => 
          // Compute the result of the merge operation for a partially defined input (either A or B is undefined for this segment)
          mergeValues(a,b)(from, to)(op)
        case _ => Seq.empty // a and b start at the same time. Nothing to do
        }
      } ++ {  
        // Merge the two values over the overlapping domain of definition of a and b.
        (Math.max(a.timestamp, b.timestamp), Math.min(a.definedUntil, b.definedUntil)) match {
        case (from, to) if (from < to) => mergeValues(a,b)(from,to)(op)
        case _ => throw new IllegalArgumentException("This function cannot merge non-overlapping entries.")
        }
      } ++ {
        // Handle trailing 'partial' definition
        (Math.min(a.definedUntil(), b.definedUntil()), Math.max(a.definedUntil(), b.definedUntil())) match {
          case (from, to) if (from != to) => mergeValues(a,b)(from, to)(op)
          case _ => Seq.empty; // Entries end at the same time, nothing to do.
        }
      }
  
  /** Merge two entries that have a disjoint domain. 
   *  The merge operator will be applied to each individually */
  protected def mergeDisjointDomain[A,B,R]
    (a: TSEntry[A], b: TSEntry[B])
    (op: (Option[A], Option[B]) => Option[R])
    : Seq[TSEntry[R]] =
      if (a.overlaps(b))
        throw new IllegalArgumentException("Function cannot be applied to overlapping entries.")
      else
        { op(Some(a.value), None).map(TSEntry(a.timestamp, _, a.validity)).toSeq ++ 
          emptyApply(Math.min(a.definedUntil, b.definedUntil), Math.max(a.timestamp, b.timestamp))(op).toSeq ++ 
          op(None, Some(b.value)).map(TSEntry(b.timestamp, _, b.validity)).toSeq
        }.sortBy(_.timestamp)
  
  private def emptyApply[A,B,R]
    (from: Long, to: Long)
    (op: (Option[A], Option[B]) => Option[R])
    : Option[TSEntry[R]] =
      if(from == to)
        None
      else
        op(None, None).map(TSEntry(from, _, to - from))
      
  /** Merge two entries.
   *  The domain covered by the returned entries (including a potential discontinuities)
   *  will be between min(a.timestamp, b.timestamp) and max(a.definedUntil, b.definedUntil) */
  def merge[A,B,R]
    (a: TSEntry[A], b: TSEntry[B])
    (op: (Option[A], Option[B]) => Option[R])
    : Seq[TSEntry[R]] = 
      if(!a.overlaps(b))
         mergeDisjointDomain(a, b)(op)
      else 
         mergeOverlapping(a,b)(op)
  
  /** Merge two TSEntries each containing an Either[A,B]
   *  Simply calls the normal merge function after determining which of the entries contains what type. */
  def mergeEithers[A,B,R]
    (a: TSEntry[Either[A,B]], b: TSEntry[Either[A,B]])
    (op: (Option[A], Option[B]) => Option[R])
    : Seq[TSEntry[R]] =
      (a,b) match {
      case (TSEntry(tsA, Left(valA), dA), TSEntry(tsB, Right(valB), dB)) => 
        merge(TSEntry(tsA, valA, dA), TSEntry(tsB, valB, dB))(op)
      case (TSEntry(tsB, Right(valB), dB), TSEntry(tsA, Left(valA), dA)) =>
        merge(TSEntry(tsA, valA, dA), TSEntry(tsB, valB, dB))(op)
      case _ => throw new IllegalArgumentException(s"Can't pass two entries with same sided-eithers: $a, $b")
      }
  
  /** Merge the 'single' TSEntry to the 'others'.
   *  The domain of definition of the 'single' entry is used: 
   *  non-overlapping parts of the other entries will not be merged.*/
  def mergeSingleToMultiple[A,B,R]
    (single: TSEntry[Either[A,B]], others: Seq[TSEntry[Either[A,B]]])
    (op: (Option[A], Option[B]) => Option[R])
    : Seq[TSEntry[R]] =
    others.collect{ 
        // Retain only entries overlapping with 'single', and constraint them to the 'single' domain.
        case e: TSEntry[_] if(single.overlaps(e)) => 
          e.trimEntryLeftNRight(single.timestamp, single.definedUntil)
      } match {
        // Merge remaining constrained entries
        case Seq() => mergeEitherToNone(single)(op).toSeq
        case Seq(alone) => mergeEithers(single, alone.trimEntryLeftNRight(single.timestamp, single.definedUntil))(op)
        case toMerge: Seq[_] =>
          toMerge.head
          // Take care of the potentially undefined domain before the 'others'
          mergeDefinedEmptyDomain(single)(single.timestamp, toMerge.head.timestamp)(op) ++ 
          // Merge the others to the single entry, including potential undefined spaces between them.
          // Group by pairs of entries to be able to trim the single one to the relevant domain
          // for the individual merges   
          toMerge.sliding(2)
                 .flatMap(p => 
                   mergeEithers(single.trimEntryLeftNRight(p.head.timestamp, p.last.timestamp), p.head)(op)) ++
          // Take care of the last entry and the potentially undefined domain after it and the end of the single one.
          mergeEithers(toMerge.last, single.trimEntryLeft(toMerge.last.timestamp))(op)
    }
  
  /** Merge the provided entry to a None on the domain spawned by [from, until[*/
  private def mergeDefinedEmptyDomain[A, B, R]
    (e: TSEntry[Either[A,B]])
    (from: Long, until: Long)
    (op: (Option[A], Option[B]) => Option[R])
    : Seq[TSEntry[R]] =
      if(from == until)
        Seq.empty
      else
        mergeEitherToNone(e.trimEntryLeftNRight(from, until))(op).toSeq
  
  /** Convenience function to merge the values present in the entries at time 'at' and
   *  create an entry valid until 'until' from the result, if the merge operation is defined
   *  for the input. */
  private def mergeValues[A, B, R]
    (a: TSEntry[A], b: TSEntry[B])
    (at: Long, until: Long)
    (op: (Option[A], Option[B]) => Option[R])
    : Seq[TSEntry[R]] = 
      op(a.at(at), b.at(at)).map(TSEntry(at, _ , until - at)).toSeq
      
  /** Merge the provided entry to a None, using the specified operator*/
  def mergeEitherToNone[A, B, R]
    (e: TSEntry[Either[A,B]])
    (op: (Option[A], Option[B]) => Option[R])
    : Option[TSEntry[R]] =
        {e match {
          case TSEntry(t, Left(valA), d) => op(Some(valA), None)
          case TSEntry(t, Right(valB), d) => op(None, Some(valB))
        }}.map(TSEntry(e.timestamp, _, e.validity))
      
}