# tslib -- a lightweight time series library

Easily manipulate and query time-series like data. Useful for manipulating series of discrete values associated to a validity or time-to-live duration, like sensor measures for example.

This library exposes time series as functions that have a value depending on time. Time series may also not be defined for certain time intervals. Additionally, you may apply operators between time series, the simple ones being addition, subtraction, while custom ones that can be applied to any type are easy to apply.

This library is not intended to provide in-depth statistics about time series data, only to make manipulating and querying it easy. 

## Examples

### Querying 

### Basic Operations

### Custom Operators: Time Series Merging

## Why 
I've had to handle time-series like data in Java recently, which turned out to be ~~slightly~~ really frustrating.

Having some spare time and wanting to see what I could come up with in Scala, I decided to build a small time series library. Additional reasons are:

  - It's fun
  - There seems to be no library doing something like that out there
  - I wanted to write some Scala again.

## TODOS
  - updatable timeseries (ala immutable collection, or mutable collection style)
  - compression (at least for strict equality) (only really makes sense for mutable timeseries)
  - review trait function implementations for efficiency? (ie, split/slice. Slice at least could be a stupid wrapper checking the bounds?)
  - check if sorting of sequences for merges is required/efficient
  - decent tests for non-trivial merge operators
  - default Seq implementation (and the one that is imported) is mutable -> consider the implications and see if we can easily fix this by 'import scala.collection.immutable.Seq' everywhere required.
  - input validation when applying. Check entries sorted (for the vector TS) and without overlap.
  - Have empty time series always be represented by an EmptyTimeSeries. (Ie, wrapping an empty vector or map with a vector/treemap time-series should not happen)
  - Generic tests for any kind of TS implementation
  - benchmarks to actually compare various implementations.
  - make it easy to use from Java
