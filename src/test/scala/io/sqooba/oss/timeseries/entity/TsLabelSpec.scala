package io.sqooba.oss.timeseries.entity

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Failure, Try}

class TsLabelSpec extends AnyFlatSpec with Matchers {

  "TsLabel.unit" should "be derived from label name" in {

    implicit val physicsParser: LabelUnitMapper = new LabelUnitMapper {
      private val AMPERE_PATTERN   = "(.*_A_.*)".r
      private val HERTZ_PATTERN    = "(.*_Hz_.*)".r
      private val PHS_VOLT_PATTERN = "(.*_PhsV_.*)".r
      private val TEMP_PATTERN     = "(.*Temp_.*)".r

      // scalastyle:off non.ascii.character.disallowed
      def deriveUnit(in: TsLabel): Option[String] =
        in.value match {
          case AMPERE_PATTERN(_)   => Some("A")
          case HERTZ_PATTERN(_)    => Some("Hz")
          case PHS_VOLT_PATTERN(_) => Some("V")
          case TEMP_PATTERN(_)     => Some("°C")
          case _                   => None
        }
    }

    Seq(
      ("MMCX_A_10m_Avg", Some("A")),
      ("MMCX_Hz_10m_Avg", Some("Hz")),
      ("MMCX_PhsV_PhsA_10m_Avg", Some("V")),
      ("WCNV_XXTemp_10m_Avg", Some("°C")),
      ("MMCX_PF_10m_Avg", None),
      ("this_is_an_unkown_unit", None)
    ).foreach {
      case (label, unit) => TsLabel(label).unit shouldBe unit
    }
  }
  // scalastyle:on non.ascii.character.disallowed

  it should "return a failure if a case is not handled" in {

    implicit val parser: LabelUnitMapper = (in: TsLabel) =>
      in.value match {
        case "not exhaustive" => Some("unit")
        case _                => None
      }

    TsLabel("anything").unit shouldBe None
    TsLabel("not exhaustive").unit shouldBe Some("unit")
  }
}
