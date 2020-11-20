---
layout: home
title:  "Home"
section: "home"
technologies:
---

[![Build Status](https://travis-ci.com/Sqooba/scala-timeseries-lib.svg?branch=master)](https://travis-ci.com/Sqooba/scala-timeseries-lib)
[![Coverage Status](https://coveralls.io/repos/github/Sqooba/scala-timeseries-lib/badge.svg?branch=master)](https://coveralls.io/github/Sqooba/scala-timeseries-lib?branch=master)

## Everything is a step function [^1]

Easily manipulate and query time series data of any type. Useful for manipulating
series of discrete values associated to a validity or time-to-live duration, like
sensor measures.

#### High level features

- Exposes time series as functions that have a value depending on time
- Time series may be undefined for certain time intervals
- Operators may be applied between time series, going from simple `+` and `*`
  to user-defined functions between any type
- Compression of numerical time series with the Gorilla TSC algorithm

#### Explicit non-goals

This library is not intended to provide in-depth statistics about time series data,
only to make manipulating and querying it easy, without any kind of approximation.

[^1]: It makes our mathematician friends go mad. Every. Single. Time.
