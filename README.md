# scala-timeseries-lib — a lightweight time series library

Easily manipulate and query time-series like data. Useful for manipulating series of discrete values associated to a validity or time-to-live duration, like sensor measures for example.

This library exposes time series as functions that have a value depending on time. Time series may also be undefined for certain time intervals. Operators may be applied between time series, the simple ones being addition and multiplication, while custom ones that can be applied between arbitrary types are easy to implement.

This library is not intended to provide in-depth statistics about time series data, only to make manipulating and querying it easy, without any kind of approximation.

## Usage
In essence, a TimeSeries is just an ordered map of `[Long,T]`. In most use cases the Key represents the time since the epoch in milliseconds, but the implementation makes no assumption about the time unit of the key.


### Defining a Timeseries 
The TimeSeries trait has two main implementations: `VectorTimeSeries[T]` and `TreeMapTimeSeries[T]`, referring to the underlying collection holding the data.

```
val tsv = VectorTimeSeries(
              1000L -> ("One", 1000L),   // String 'One' lives at 1000 on the timeline and is valid for 1000.
              2000L -> ("Two", 1000L),
              4000L -> ("Four", 1000L))
              
val tst = TreeMapTimeSeries(
              1000L -> ("One", 1000L), 
              2000L -> ("Two", 1000L),
              4000L -> ("Four", 1000L))
```
`tsv` and `tst` now both define a time series of Strings that is defined on the `[1000,5000[` interval, with a hole at `[3000,4000[`

### Querying
The simplest function exposed by a timeseries is `at(t: Long): Option[T]`. With `ts` defined as above, calling `at()`yields the following results:

```
    ts.at(999)  // None
    ts.at(1000) // Some("One")
    ts.at(1999) // Some("One")
    ts.at(2000) // Some("Two")
    ts.at(3000) // None
    ts.at(3999) // None
    ts.at(4000) // Some("Four")
    ts.at(4999) // Some("Four")
    ts.at(5000) // None
```
### Basic Operations
TimeSeries of any `Numeric` type come with basic operators you might expect for such cases:

```
val tsa = VectorTimeSeries(
        0L -> (1.0, 10L),
        10L -> (2.0, 10L))  
        
val tsb = VectorTimeSeries(
        0L -> (3.0, 10L),
        10L -> (4.0, 10L))
        
tsa + tsb // (0L -> (4.0, 10l), 10L -> (6.0, 10L))
tsa * tsb // (0L -> (3.0, 10l), 10L -> (8.0, 10L))

```

Note that there are a few quirks to be aware of when a TimeSeries has discontinuities: please refer to function comments in `NumericTimeSeries.scala` (living in `ch.shastick`) for more details.

### Custom Operators: Time Series Merging
For non-numeric TimeSeries, or for any particular needs, TimeSeries can be merged using an arbitrary merge operator: `op: (Option[A], Option[B]) => Option[C]`. For example:

```
def plus(aO: Option[Double], bO: Option[Double]) = 
    (aO, bO) match {
      // Wherever both timeseries share a defined domain, return the sum of the values
      case (Some(a), Some(b)) => Some(a+b) 
      // Wherever only a single timeseries is defined, return the defined value
      case (Some(a), None) => aO
      case (None, Some(b)) => bO
      // Where none of the timeseries are defined, the result remains undefined.
      case _ => None
    }
```

For a complete view of what you can do with a TimeSeries, the best is to have a look at the `TimeSeries.scala` trait living in `ch.shastick`.

### Under the hood
While a TimeSeries looks a lot like an ordered `Map[Long,T]`, it should more be considered like an ordered collection of triples of the form `(timestamp, value, validity)` (called a `TSEntry[T]` internally), representing small timeseries chunks.
## Notes on Performance

As suggested by their name, `VectorTimeSeries` and `TreeMapTimeSeries` have different underlying collections holding the data. Depending on the use-case, one or the other might be preferable:

  - `TreeMapTimeSeries` should be preferred for query-intensive tasks that seek for arbitrary points in time within the timeseries, like `at(), split(), slice()` as searching for something in a `Vector` implies a linear search.
  - `VectorTimeSeries` can be used for everything else, especially when it implies iterating over whole timeseries (generally `merge()`, `append()` or `prepend()` related operations), which often needs to be done while cooking up a TimeSeries using the content of other ones.
  
Also note that the current implementation for merging timeseries probably deserves some improvements.

## Why 
I've had to handle time-series like data in Java recently, which turned out to be ~~slightly~~ really frustrating.

Having some spare time and wanting to see what I could come up with in Scala, I decided to build a small time series library. Additional reasons are:

  - It's fun
  - There seems to be no library doing something like that out there
  - I wanted to write some Scala again.

## TODOS
  - updatable timeseries (ala mutable collection style)
  - compression (at least for strict equality) when new entries are appended
  - review trait function implementations for efficiency? (ie, split/slice. Slice at least could be a stupid wrapper checking the bounds?)
  - check if sorting of sequences for merges is required/efficient.
  - decent tests for non-trivial merge operators
  - default Seq implementation (and the one that is imported) is mutable -> consider the implications and see if we can easily fix this by 'import scala.collection.immutable.Seq' everywhere required.
  - input validation when applying. Check entries sorted (for the vector TS) and without overlap.
  - Have empty time series always be represented by an EmptyTimeSeries. (Ie, wrapping an empty vector or map with a vector/treemap time-series should not happen)
  - Generic tests for any kind of TS implementation
  - benchmarks to actually compare various implementations.
  - make it easy to use from Java
