package org.apache.spark.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
  * Created by peng on 10/06/16.
  */
abstract class DelegateInternalRow extends InternalRow {

  val data: InternalRow

//  import data._

  override def numFields: Int = data.numFields

  override def anyNull: Boolean = data.anyNull

  override def copy(): InternalRow = data.copy()

  override def get(ordinal: Int, dataType: DataType): AnyRef = data.get(ordinal, dataType)

  override def getUTF8String(ordinal: Int): UTF8String = data.getUTF8String(ordinal)

  override def getBinary(ordinal: Int): Array[Byte] = ???

  override def getDouble(ordinal: Int): Double = ???

  override def getArray(ordinal: Int): ArrayData = ???

  override def getInterval(ordinal: Int): CalendarInterval = ???

  override def getFloat(ordinal: Int): Float = ???

  override def getLong(ordinal: Int): Long = ???

  override def getMap(ordinal: Int): MapData = ???

  override def getByte(ordinal: Int): Byte = ???

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = ???

  override def getBoolean(ordinal: Int): Boolean = ???

  override def getShort(ordinal: Int): Short = ???

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = ???

  override def getInt(ordinal: Int): Int = ???

  override def isNullAt(ordinal: Int): Boolean = ???
}
