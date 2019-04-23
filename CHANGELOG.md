# Change log

## 3.1.2

- Moving to `kaeter` to manage versioning, skipped 3.1.1.

## 3.1.0

### New features

- New `nonStrictPlus` function that will let you do sums over time domains that are not perfectly aligned
- New //discrete// trimming, slicing and splitting to avoid the modification of existing entries in a time series. Useful when dealing with aggregates for which splitting an entry makes no sense.

### Bug fixes:

- Resampling is not subject to stack overflows anymore

## 3.0.0

### Breaking retro-compatibility:

- `EmptyTimeSeries` is now an object instead of a class
- `TimeSeriesBuilder` has two significant changes:

    - It throws an exception if entries' timestamp are not strictly increasing
    - Its type has changed from `Builder[TSEntry, Vector[TSEntry]]` to `Builder[TSEntry, TimeSeries]`,
      thus the `result` method has a different signature

- `VectorTimeSeries`'s constructor is not publicly available anymore, methods of `TimeSeries` companion
  object should be used instead.
- `VectorTimeSeries` must contain at least two elements, otherwise `TSEntry` or `EmptyTimeSeries` should be used
- `TSValue` has been removed
- Removed parenthesis in methods having neither an argument nor a side-effect

### New features:

- Created `TimeDomain` to represent a set of timestamps using our conventions
- Created `looseDomain` method that returns an approximation of the domain. By loose-domain, we mean
  the tight-domain on which we know that the time-series is not defined outside but might be defined
  inside.
- Added `TimeSeries.fallback` which takes another time-series and returns another time-series which
  has the value of `this` if it's defined, and the value of the argument otherwise
- Added `isEmpty` and `nonEmpty` methods in time-series

### Bug fixes:

- `mergeEithers` fixed when dealing with time-series that have the same undefined internal domain
- `append`/`prepend` now return compressed time-series
