# tslib -- a lightweight time series library

## Why 
I've had to handle time-series like data in Java recently, which turned out to be slightly frustrating due to Java's limitations. 

Having some spare time and wanting to see what I could come up with in Scala, I decided to build a small time series library. Additional reasons are:

  - It's fun
  - There seems to be no library doing something like that out there
  - I wanted to write some Scala again.

## TODOS
  - updatable timeseries (ala immutable collection, or mutable collection style)
  - compression (at least for strict equality)
  - slicing
  - decent tests for non-trivial merge operators
