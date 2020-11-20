# Change log

## 1.4.0 -

## 1.3.1 - 28.1.19
* [deps] remove shapeless from the mere (though we still depend on it)
* [fix] `EmptyTimeSeries.rollup()` now properly returns an `EmptyTimeSeries` instead of throwing with `head of empty Stream`

## 1.3.0 - 25.11.19
* [feature] Add sliding window methods on time series trait.
* [internal] Reduce number of abstract functions in time series trait and reduce boilerplate.
* [fix] Fix a stack overflow bug in the merge operation.

## 1.2.0 - 6.11.19
* [feature] Adapt sliding integral.
* [feature] Rework window aggregation and add dynamic windowing.
* [internal] Shapeless merge for pairs.

Note: this release introduces a dependency on shapeless. We are aware that this may be slightly suboptimal.
We are experimenting with it for an upcomming feature that would allow to merge an arbitrary number of series.

Depending on future choices, that dependency may be removed in the future.

## 1.1.0 - 22.10.19

* [fix] fix flapping test
* [feature] add convenience function `values`
* [feature] add tooling to compute basic statistics using reservoir sampling
* [feature] useful `rollup` function for rollup/decimation
* [fix] Fix build and tests under Scala 2.12
* [fix] fix validation bug that prevented series from having big gaps

## 1.0.0 - 16.10.19

Let this library appear.
