# scala-timeseries-lib

[![Build Status](https://travis-ci.com/Sqooba/scala-timeseries-lib.svg?branch=master)](https://travis-ci.com/Sqooba/scala-timeseries-lib)
[![Coverage Status](https://coveralls.io/repos/github/Sqooba/scala-timeseries-lib/badge.svg?branch=master)](https://coveralls.io/github/Sqooba/scala-timeseries-lib?branch=master)

> Lightweight, functional and exact time series library for scala

See the [microsite](https://sqooba.github.io/scala-timeseries-lib/) for more information and documentation.

## TL;DR

```sbt
// https://mvnrepository.com/artifact/io.sqooba.oss/scala-timeseries-lib
libraryDependencies += "io.sqooba.oss" %% "scala-timeseries-lib" % "1.1.0"
```

or, if you want to cook your own, local, `HEAD-SNAPSHOT` release, just

```bash
make release-local
```

Alternatively, if you want to set a specific version when installing locally:
```bash
make -e VERSION=1.0.0 release-local
```

## Usage

In essence, a `TimeSeries` is just an ordered map of `[Long,T]`. In most use cases the key represents the time
since the epoch in milliseconds, but the implementation makes no assumption about the time unit of the key.


### Defining a `TimeSeries`

The `TimeSeries` trait has a default implementation: `VectorTimeSeries[T]`, referring to the underlying collection holding the data.
There are other implementations as well.

```scala
val ts = TimeSeries(Seq(
  TSEntry(1000L, "One",  1000L),   // String 'One' lives at 1000 on the timeline and is valid for 1000.
  TSEntry(2000L, "Two",  1000L),
  TSEntry(4000L, "Four", 1000L)
))
```

`ts` now defines a time series of `String` that is defined on the interval `[1000,5000[`, with a hole at `[3000,4000[`

The `TimeSeries.apply` contstructor is quite expensive because it sorts the entries to ensure a sane series.
Usually, the input is already sorted. In that case there are two other constructors:

- `TimeSeries.ofOrderedEntriesSafe`: This checks whether the entries are in correct order and trims them so as to not
  overlap. It can optionally compress the entries.

- `TimeSeries.ofOrderedEntriesUnsafe`: This doesn't do any checks, trims or compression on the data and
   just wraps them in a time series.

- `TimeSeries.newBuilder`: returns a builder to incrementally build a new time series.

### Querying
The simplest function exposed by a time series is `at(t: Long): Option[T]`. With `ts` defined as above, calling `at()`
yields the following results:

```scala
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

```scala
val tsa = TimeSeries(Seq(
  TSEntry(0L,  1.0, 10L),
  TSEntry(10L, 2.0, 10L)
)
val tsb = TimeSeries(Seq(
  TSEntry(0L,  3.0, 10L),
  TSEntry(10L, 4.0, 10L)
)

tsa + tsb // (TSEntry(0, 4.0, 10L), TSEntry(10, 6.0, 10L))
tsa * tsb // (TSEntry(0, 3.0, 10L), TSEntry(10, 8.0, 10L))
```

Note that there are a few quirks to be aware of when a TimeSeries has discontinuities:
please refer to function comments in
[`NumericTimeSeries.scala`](src/main/scala/io/sqooba/oss/timeseries/NumericTimeSeries.scala) for more details.

### Custom Operators: time series Merging
For non-numeric time series, or for any particular needs, a `TimeSeries` can be merged using an
arbitrary merge operator: `op: (Option[A], Option[B]) => Option[C]`. For example (this method is already defined
for you in the interface, no need to rewrite it):

```scala
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

For a complete view of what you can do with a `TimeSeries`,
the best is to have a look at the [`TimeSeries.scala`](src/main/scala/io/sqooba/oss/timeseries/TimeSeries.scala) interface.

### Under the hood
While a `TimeSeries[T]` looks a lot like an ordered `Map[Long,T]`, it should more be considered like an ordered
collection of triples of the form `(timestamp: Long, value: T, validity: Long)` (called a `TSEntry[T]` internally),
representing small, constant, time series chunks.

Essentially, it's a step function.

## Notes on Performance

The original goal was to provide abstractions that are easy to use and to understand.

While we still strive to keep the library simple to use, we are also shifting to more intensive applications:
performance is thus becoming more of a priority.

### Details
As suggested by its name, `VectorTimeSeries` is backed by a `Vector` and uses dichotomic search for lookups.
The following performances can thus be expected (using the denomination
[found here](http://docs.scala-lang.org/overviews/collections/performance-characteristics.html)):

  - `Log` for random lookups, left/right trimming and slicing within the definition bounds
  - `eC` (effectively constant time) for the rest (appending, prepending, head, last, ...)

Each data point is however represented by an object, which hurts memory usage. Therefore there is a second
implementation: `ColumnTimeSeries` which represents its entries with a column-store of three vectors
`(Vector[Long], Vector[T], Vector[Long])`. This should save space for primitive types.

You can create a `ColumnTimeSeries` with its builder `ColumnTimeSeries.newBuilder`.

# Misc

### Why
I've had to handle time series like data in Java in the past, which turned out to be ~~slightly~~ really frustrating.

Having some spare time and wanting to see what I could come up with in Scala, I decided to build a small time series
library. Additional reasons are:

  - It's fun
  - There seems to be no library doing something like that out there
  - I wanted to write some Scala again.

Since then, we began using this for smaller projects at [Sqooba](https://sqooba.io/) and maintenance has officially
been taken over in May 2019.

### TODOS
  - stream/lazy-collections implementation
  - more tests for non-trivial merge operators
  - benchmarks to actually compare various implementations
  - make it easy to use from Java
  - consider https://scalacheck.org/ for property-based testing?
  - interoperability with something like Apache Arrow?

### Publish the microsite

You need write access on the Github repo to push the microsite. The site is built
entirely with `sbt` plugins and lives on the `gh-pages` branch. You can edit it in
[`/docs`](docs/index.md). Scaladoc is automatically created.

In order to build the site locally you need `sbt` version `1.3.3+` and `jekyll`
version `3.8.5+` (the [installation on
macOS](https://jekyllrb.com/docs/installation/macos/#rbenv) is a bit tricky because
or ruby.)

Once you have this, you can

```bash
sbt makeMicrosite && jekyll serve -s target/site
```
You can now visit the site under http://localhost:4000/scala-timeseries-lib. To publish, just
```bash
sbt publishMicrosite
```

If you are having trouble with an error like this: `fatal: not a git repository `,
check [that](https://github.com/sbt/sbt-ghpages/issues/40).


### Contributions
First and foremost: contributions are more than welcome!

We manage this library on an internal repository, which gets synced to github. However, we are able to support the
classic github PR workflow, so you should normally be able to ignore our setup's particularities.

# Contributors

- [Shastick](https://github.com/Shastick) - Maintainer
- [fdevillard](https://github.com/fdevillard)
- [nsanglar](https://github.com/nsanglar)
- [yannbolliger](https://github.com/yannbolliger)

# Changelog

Please refer to the [CHANGELOG](CHANGELOG.md)
