package io.sqooba.oss.timeseries.utils

import java.util.Base64

import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec}
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.transport.TMemoryBuffer

import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

/**
  * A class packing the logic to serialize and deserialize thrift structures
  * of a given type to/from raw byte arrays or base64 strings.
  *
  * This class is THREAD SAFE: a new memory buffer is allocated for each function call.
  */
class ThriftMarshaller[T <: ThriftStruct](
    companion: ThriftStructCodec[T], // the companion object for this serializer's type
    protocolFactory: TProtocolFactory // the protocol factory to be used
) {

  def encode(struct: T): Try[Array[Byte]] =
    Try {
      val buf   = new TMemoryBuffer(32)
      val proto = protocolFactory.getProtocol(buf)
      companion.encode(struct, proto)
      buf.getArray
    }

  def encodeToBase64(struct: T): Try[String] =
    encode(struct).map(Base64.getEncoder.encodeToString)

  def decode(bytes: Array[Byte]): Try[T] =
    Try {
      val buf = new TMemoryBuffer(bytes.length)
      buf.write(bytes, 0, bytes.length)
      val proto = protocolFactory.getProtocol(buf)
      companion.decode(proto)
    }

  def decodeFromBase64(s: String): Try[T] =
    Try(Base64.getDecoder.decode(s)).flatMap(decode)

}

object ThriftMarshaller {

  /**
    * Thank you, stackoverflow, for your unlimited supply of dirty scala tricks:
    * https://stackoverflow.com/questions/9172775/get-companion-object-of-class-by-given-generic-type-scala
    */
  import scala.reflect.runtime.{currentMirror => cm}

  /**
    * @return the companion object of any defined thrift structure. Mainly used
    *         for getting general access to encode/decode functions without knowing
    *         the exact type of an object to serialize/deserialize.
    */
  def companionOf[T <: ThriftStruct](implicit tag: TypeTag[T]): Try[ThriftStructCodec[T]] = {
    Try {
      val companionModule = tag.tpe.typeSymbol.companion.asModule
      cm.reflectModule(companionModule).instance.asInstanceOf[ThriftStructCodec[T]]
    }
  }

  /**
    * @return a new serializer for the inferred type using the specified protocol factory.
    */
  def forType[T <: ThriftStruct](protocolFactory: TProtocolFactory)(implicit tag: TypeTag[T]): Try[ThriftMarshaller[T]] =
    companionOf[T].map(comp => new ThriftMarshaller(comp, protocolFactory))
}
