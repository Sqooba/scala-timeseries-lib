package io.sqooba.oss.timeseries.entity

import scala.annotation.implicitNotFound

/** Provide a mapping from label to units: Implementations may provide any logic (either
  * through a static mapping or deriving it from the label itself) to return an optional
  * unit for a given label.
  */
@implicitNotFound("Cannot derive units for TsLabel because no LabelUnitMapper was defined.")
trait LabelUnitMapper {

  /**
    * @param in the label for which the unit should be determined
    * @return the unit corresponding to this label, if any can be derived.
    */
  def deriveUnit(in: TsLabel): Option[String]

}
