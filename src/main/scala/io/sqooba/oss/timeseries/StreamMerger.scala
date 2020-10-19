package io.sqooba.oss.timeseries

object StreamMerger {

  /** Same as [[TimeSeriesMerger.mergeOrderedSeqs]] but for ZIO streams.
    *
    * The recursive function design is inspired by the implementation of
    * [[zio.stream.ZStream#interleaveWith]].
    *
    * @param leftStream ordered
    * @param rightStream ordered
    * @return stream that contains all elements of left and right in the correct order
    */
  //  private[timeseries] def mergeOrderedSeqs[E, A](
  //      leftStream: Stream[E, A],
  //      rightStream: Stream[E, A]
  //  )(implicit o: Ordering[A]): Stream[E, A] = {
  //    import o._
  //
  //    def loop(
  //        leftTake: Option[Take[E, A]],
  //        rightTake: Option[Take[E, A]],
  //        leftPull: Pull[Any, E, A],
  //        rightPull: Pull[Any, E, A]
  //    ): IO[E, ((Option[Take[E, A]], Option[Take[E, A]]), Take[E, A])] = (leftTake, rightTake) match {
  //      // both stopped so we stop
  //      case (Some(Take.end), Some(Take.end)) => IO.succeed((leftTake, rightTake), Take.end)
  //
  //      // errors get forwarded
  //      case (Some(Fail(e)), _) => IO.succeed((None, None), Take.fail(e))
  //      case (_, Some(Fail(e))) => IO.succeed((None, None), Take.fail(e))
  //
  //      // If one side is empty, we evaluate it and recurse
  //      // TODO: optimise with parallel evaluation if both takes are not present
  //      case (None, _) => Take.fromPull(leftPull).flatMap(l => loop(Some(l), rightTake, leftPull, rightPull))
  //      case (_, None) =>
  //        Take.fromPull(rightPull).flatMap(r => loop(leftTake, Some(r), leftPull, rightPull))
  //
  //      // If one has ended, we always take the other
  //      case (Some(Take.end), Some(Take(Success(r)))) => IO.succeed((leftTake, None), Take.single(r))
  //      case (Some(Take(Success(l))), Some(Take.end)) => IO.succeed((None, rightTake), Take.single(l))
  //
  //      // If both are present, we take the smaller
  //      case (Some(Take(Success(NonEmptyChunk))), Some(Take(Success(r)))) if l <= r =>
  //        IO.succeed((None, rightTake), Take.single(l))
  //
  //      case (Some(Take(Success(_))), Some(Take(Success(Chunk(r))))) =>
  //        IO.succeed((leftTake, None), Take.single(r))
  //    }
  //
  //    leftStream.combine(rightStream)(
  //      (Option.empty[Take[E, A]], Option.empty[Take[E, A]])
  //    ) {
  //      case ((leftTake, rightTake), leftPull, rightPull) => loop(leftTake, rightTake, leftPull, rightPull)
  //    }
  //  }

}
