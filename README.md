# scala-timeseries-lib [![Build Status](https://travis-ci.com/Sqooba/scala-timeseries-lib.svg?branch=master)](https://travis-ci.com/Sqooba/scala-timeseries-lib) [![Coverage Status](https://coveralls.io/repos/github/Sqooba/scala-timeseries-lib/badge.svg?branch=master)](https://coveralls.io/github/Sqooba/scala-timeseries-lib?branch=master)
> Lightweight, functional and exact time-series library for scala

Easily manipulate and query time-series like data. Useful for manipulating series of discrete values associated to a validity or time-to-live duration, like sensor measures.

#### High level features:

- Exposes time series as functions that have a value depending on time
- Time series may be undefined for certain time intervals
- Operators may be applied between time series, the simple ones being addition and multiplication, while custom ones that can be applied between arbitrary types are easy to implement.

#### Explicit non-goals

This library is not intended to provide in-depth statistics about time series data, only to make manipulating and querying it easy, without any kind of approximation.

## Usage
In essence, a `TimeSeries` is just an ordered map of `[Long,T]`. In most use cases the Key represents the time since the epoch in milliseconds, but the implementation makes no assumption about the time unit of the key.


### Defining a `TimeSeries`
The `TimeSeries` trait has one main implementation: `VectorTimeSeries[T]`, referring to the underlying collection holding the data.

```
val tsv = VectorTimeSeries(
              1000L -> ("One", 1000L),   // String 'One' lives at 1000 on the timeline and is valid for 1000.
              2000L -> ("Two", 1000L),
              4000L -> ("Four", 1000L))
              
```
`tsv` now defines a time series of Strings that is defined on the `[1000,5000[` interval, with a hole at `[3000,4000[`

### Querying
The simplest function exposed by a time series is `at(t: Long): Option[T]`. With `ts` defined as above, calling `at()`yields the following results:

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
`TimeSeries` of any `Numeric` type come with basic operators you might expect for such cases:

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
For non-numeric time series, or for any particular needs, a `TimeSeries` can be merged using an arbitrary merge operator: `op: (Option[A], Option[B]) => Option[C]`. For example:

```
def plus(aO: Option[Double], bO: Option[Double]) = 
    (aO, bO) match {
      // Wherever both time series share a defined domain, return the sum of the values
      case (Some(a), Some(b)) => Some(a+b) 
      // Wherever only a single time series is defined, return the defined value
      case (Some(a), None) => aO
      case (None, Some(b)) => bO
      // Where none of the time series are defined, the result remains undefined.
      case _ => None
    }
```

For a complete view of what you can do with a `TimeSeries`, the best is to have a look at the [`TimeSeries.scala`](src/main/scala/io/sqooba/timeseries/TimeSeries.scala) interface.

### Under the hood
While a `TimeSeries[T]` looks a lot like an ordered `Map[Long,T]`, it should more be considered like an ordered collection of triples of the form `(timestamp, value, validity)` (called a `TSEntry[T]` internally), representing small, constant, time series chunks.

Essentially, it's a step function.

## Notes on Performance

The original goal was to provide abstractions that are easy to use and to understand. 

While we still strive to keep the library simple to use, we are also shifting to more intensive applications: performance is thus becoming more of a priority.

### Details
As suggested by its name, `VectorTimeSeries` is backed by a `Vector` and uses dichotomic search for lookups. The following performances can thus be expected (using the denomination [found here](http://docs.scala-lang.org/overviews/collections/performance-characteristics.html)):

  - `Log` for random lookups, left/right trimming and slicing within the definition bounds
  - `eC` (effectively constant time) for the rest (appending, prepending, head, last, ...)

Each data point is however represented by an object, which kinda hurts memory usage: improving this is on the roadmap.

# Misc

### Why 
I've had to handle time series like data in Java in past, which turned out to be ~~slightly~~ really frustrating.

Having some spare time and wanting to see what I could come up with in Scala, I decided to build a small time series library. Additional reasons are:

  - It's fun
  - There seems to be no library doing something like that out there
  - I wanted to write some Scala again.

Since then, we began using this for smaller projects at [Sqooba](https://sqooba.io/) and maintenance has officially been taken over in May 2019.

### TODOS
  - memory efficient implementation
  - interoperability with something like Apache Arrow
  - good serialization (e.g. Gorilla TSZ), bonus if it's compatible with Arrow (or Parquet?)
  - more tests for non-trivial merge operators
  - Generic tests for any kind of TS implementation
  - benchmarks to actually compare various implementations.
  - make it easy to use from Java
  - consider https://scalacheck.org/ for property-based testing ? (especially for ordering-related tests?)


### Contributions
First and foremost: contributions are more than welcome!

We manage this library on an internal repository, which gets synced to github. However, we are able to support the classic github PR workflow, so you should normally be able to ignore our setup's particularities.

# Contributors

- [Shastick](https://github.com/Shastick) - Maintainer
- [fdevillard](https://github.com/fdevillard)
- [nsanglar](https://github.com/nsanglar)
