/*
Copyright (c) 2005 Health Market Science, Inc.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
USA

You can contact Health Market Science at info@healthmarketscience.com
or at the following address:

Health Market Science
2700 Horizon Drive
Suite 200
King of Prussia, PA 19406
*/

package com.healthmarketscience.jackcess;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

/**
 * Access data type
 * @author Tim McCune
 */
public class DataType
{
  private static ArrayList values = new ArrayList();
  
  public static DataType BOOLEAN = new DataType((byte) 0x01, new Integer(Types.BOOLEAN), new Integer(0));
  public static DataType BYTE = new DataType((byte) 0x02, new Integer(Types.TINYINT), new Integer(1));
  public static DataType INT = new DataType((byte) 0x03, new Integer(Types.SMALLINT), new Integer(2));
  public static DataType LONG = new DataType((byte) 0x04, new Integer(Types.INTEGER), new Integer(4));
  public static DataType MONEY = new DataType((byte) 0x05, new Integer(Types.DECIMAL), new Integer(8));
  public static DataType FLOAT = new DataType((byte) 0x06, new Integer(Types.FLOAT), new Integer(4));
  public static DataType DOUBLE = new DataType((byte) 0x07, new Integer(Types.DOUBLE), new Integer(8));
  public static DataType SHORT_DATE_TIME = new DataType((byte) 0x08, new Integer(Types.TIMESTAMP), new Integer(8));
  public static DataType BINARY = new DataType((byte) 0x09, new Integer(Types.BINARY), null, true, false, new Integer(0), new Integer(255), new Integer(255), 1);
  public static DataType TEXT = new DataType((byte) 0x0A, new Integer(Types.VARCHAR), null, true, false, new Integer(0), new Integer(50 * JetFormat.TEXT_FIELD_UNIT_SIZE), new Integer(JetFormat.TEXT_FIELD_MAX_LENGTH), JetFormat.TEXT_FIELD_UNIT_SIZE);
  public static DataType OLE = new DataType((byte) 0x0B, new Integer(Types.LONGVARBINARY), null, true, true, new Integer(0), null, new Integer(0xFFFFFF), 1);
  public static DataType MEMO = new DataType((byte) 0x0C, new Integer(Types.LONGVARCHAR), null, true, true, new Integer(0), null, new Integer(0xFFFFFF), JetFormat.TEXT_FIELD_UNIT_SIZE);
  public static DataType UNKNOWN_0D = new DataType((byte) 0x0D);
  public static DataType GUID = new DataType((byte) 0x0F, null, new Integer(16));
  // for some reason numeric is "var len" even though it has a fixed size...
  public static DataType NUMERIC = new DataType((byte) 0x10, new Integer(Types.NUMERIC), null, true, false, new Integer(17), new Integer(17), new Integer(17), true, new Integer(0), new Integer(0), new Integer(28), new Integer(1), new Integer(18), new Integer(28), 1);

  /** Map of SQL types to Access data types */
  private static Map SQL_TYPES = new HashMap();
  /** Alternate map of SQL types to Access data types */
  private static Map ALT_SQL_TYPES = new HashMap();
  static
  {
    int nv = values.size();
    for (int t = 0; t < nv; t++)
    {
    	DataType type = (DataType)(values.get(t));
      if (type._sqlType != null)
      {
        SQL_TYPES.put(type._sqlType, type);
      }
    }
    SQL_TYPES.put(new Integer(Types.BIT), BYTE);
    SQL_TYPES.put(new Integer(Types.BLOB), OLE);
    SQL_TYPES.put(new Integer(Types.CLOB), MEMO);
    SQL_TYPES.put(new Integer(Types.BIGINT), LONG);
    SQL_TYPES.put(new Integer(Types.CHAR), TEXT);
    SQL_TYPES.put(new Integer(Types.DATE), SHORT_DATE_TIME);
    SQL_TYPES.put(new Integer(Types.REAL), DOUBLE);
    SQL_TYPES.put(new Integer(Types.TIME), SHORT_DATE_TIME);
    SQL_TYPES.put(new Integer(Types.VARBINARY), BINARY);

    // the "alternate" types allow for larger values
    ALT_SQL_TYPES.put(new Integer(Types.VARCHAR), MEMO);
    ALT_SQL_TYPES.put(new Integer(Types.VARBINARY), OLE);
    ALT_SQL_TYPES.put(new Integer(Types.BINARY), OLE);
  }
  
  private static Map DATA_TYPES = new HashMap();
  static
  {
    int nv = values.size();
    for (int t = 0; t < nv; t++)
    {
    	DataType type = (DataType)(values.get(t));
      DATA_TYPES.put(new Integer(type._value), type);
    }
  }

  /** is this a variable length field */
  private boolean _variableLength;
  /** is this a long value field */
  private boolean _longValue;
  /** does this field have scale/precision */
  private boolean _hasScalePrecision;
  /** Internal Access value */
  private byte _value;
  /** Size in bytes of fixed length columns */
  private Integer _fixedSize;
  /** min in bytes size for var length columns */
  private Integer _minSize;
  /** default size in bytes for var length columns */
  private Integer _defaultSize;
  /** Max size in bytes for var length columns */
  private Integer _maxSize;
  /** SQL type equivalent, or null if none defined */
  private Integer _sqlType;
  /** min scale value */
  private Integer _minScale;
  /** the default scale value */
  private Integer _defaultScale;
  /** max scale value */
  private Integer _maxScale;
  /** min precision value */
  private Integer _minPrecision;
  /** the default precision value */
  private Integer _defaultPrecision;
  /** max precision value */
  private Integer _maxPrecision;
  /** the number of bytes per "unit" for this data type */
  private int _unitSize;
  
  private DataType(byte value) {
    this(value, null, null);
  }
  
  private DataType(byte value, Integer sqlType, Integer fixedSize) {
    this(value, sqlType, fixedSize, false, false, null, null, null, 1);
  }

  private DataType(byte value, Integer sqlType, Integer fixedSize,
                   boolean variableLength,
                   boolean longValue,
                   Integer minSize,
                   Integer defaultSize,
                   Integer maxSize,
                   int unitSize) {
    this(value, sqlType, fixedSize, variableLength, longValue,
         minSize, defaultSize, maxSize,
         false, null, null, null, null, null, null, unitSize);
  }
  
  private DataType(byte value, Integer sqlType, Integer fixedSize,
                   boolean variableLength,
                   boolean longValue,
                   Integer minSize,
                   Integer defaultSize,
                   Integer maxSize,
                   boolean hasScalePrecision,
                   Integer minScale,
                   Integer defaultScale,
                   Integer maxScale,
                   Integer minPrecision,
                   Integer defaultPrecision,
                   Integer maxPrecision,
                   int unitSize) {
    _value = value;
    _sqlType = sqlType;
    _fixedSize = fixedSize;
    _variableLength = variableLength;
    _longValue = longValue;
    _minSize = minSize;
    _defaultSize = defaultSize;
    _maxSize = maxSize;
    _hasScalePrecision = hasScalePrecision;
    _minScale = minScale;
    _defaultScale = defaultScale;
    _maxScale = maxScale;
    _minPrecision = minPrecision;
    _defaultPrecision = defaultPrecision;
    _maxPrecision = maxPrecision;
    _unitSize = unitSize;
    values.add(this);
  }
  
  public byte getValue() {
    return _value;
  }
  
  public boolean isVariableLength() {
    return _variableLength;
  }

  public boolean isTrueVariableLength() {
    // some "var len" fields do not really have a variable length,
    // e.g. NUMERIC
    return (isVariableLength() && (getMinSize() != getMaxSize()));
  }
  
  public boolean isLongValue() {
    return _longValue;
  }

  public boolean getHasScalePrecision() {
    return _hasScalePrecision;
  }
  
  public int getFixedSize() {
    if(_fixedSize != null) {
      return _fixedSize.intValue();
    } else {
      throw new IllegalArgumentException("FIX ME");
    }
  }

  public int getMinSize() {
    return _minSize.intValue();
  }

  public int getDefaultSize() {
    return _defaultSize.intValue();
  }

  public int getMaxSize() {
    return _maxSize.intValue();
  }
  
  public int getSQLType() throws SQLException {
    if (_sqlType != null) {
      return _sqlType.intValue();
    } else {
      throw new SQLException("Unsupported data type: " + toString());
    }
  }

  public int getMinScale() {
    return _minScale.intValue();
  }

  public int getDefaultScale() {
    return _defaultScale.intValue();
  }
  
  public int getMaxScale() {
    return _maxScale.intValue();
  }
  
  public int getMinPrecision() {
    return _minPrecision.intValue();
  }
  
  public int getDefaultPrecision() {
    return _defaultPrecision.intValue();
  }
  
  public int getMaxPrecision() {
    return _maxPrecision.intValue();
  }

  public int getUnitSize() {
    return _unitSize;
  }

  public boolean isValidSize(int size) {
    return isWithinRange(size, getMinSize(), getMaxSize());
  }

  public boolean isValidScale(int scale) {
    return isWithinRange(scale, getMinScale(), getMaxScale());
  }

  public boolean isValidPrecision(int precision) {
    return isWithinRange(precision, getMinPrecision(), getMaxPrecision());
  }

  private boolean isWithinRange(int value, int minValue, int maxValue) {
    return((value >= minValue) && (value <= maxValue));
  }
  
  public static DataType fromByte(byte b) throws IOException {
    DataType rtn = (DataType)(DATA_TYPES.get(new Integer(b)));
    if (rtn != null) {
      return rtn;
    } else {
      throw new IOException("Unrecognized data type: " + b);
    }
  }
  
  public static DataType fromSQLType(int sqlType)
  throws SQLException
  {
    return fromSQLType(sqlType, 0);
  }
  
  public static DataType fromSQLType(int sqlType, int lengthInUnits)
  throws SQLException
  {
    DataType rtn = (DataType)(SQL_TYPES.get(new Integer(sqlType)));
    if(rtn == null) {
      throw new SQLException("Unsupported SQL type: " + sqlType);
    }

    // make sure size is reasonable
    int size = lengthInUnits * rtn.getUnitSize();
    if(rtn.isVariableLength() && !rtn.isValidSize(size)) {
      // try alternate type
      DataType altRtn = (DataType)(ALT_SQL_TYPES.get(new Integer(sqlType)));
      if((altRtn != null) && altRtn.isValidSize(size)) {
        // use alternate type
        rtn = altRtn;
      }
    }
      
    return rtn;
  }

}
