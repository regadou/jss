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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.healthmarketscience.jackcess.scsu.EndOfInputException;
import com.healthmarketscience.jackcess.scsu.Expand;
import com.healthmarketscience.jackcess.scsu.IllegalInputException;

/**
 * Access database column definition
 * @author Tim McCune
 */
public class Column implements Comparable {
  
  /**
   * Access starts counting dates at Jan 1, 1900.  Java starts counting
   * at Jan 1, 1970.  This is the # of days between them for conversion.
   */
  private static final double DAYS_BETWEEN_EPOCH_AND_1900 = 25569d;
  /**
   * Access stores numeric dates in days.  Java stores them in milliseconds.
   */
  private static final double MILLISECONDS_PER_DAY = 86400000d;
  
  /**
   * Long value (LVAL) type that indicates that the value is stored on the same page
   */
  private static final byte LONG_VALUE_TYPE_THIS_PAGE = (byte) 0x80;
  /**
   * Long value (LVAL) type that indicates that the value is stored on another page
   */
  private static final byte LONG_VALUE_TYPE_OTHER_PAGE = (byte) 0x40;
  /**
   * Long value (LVAL) type that indicates that the value is stored on multiple other pages
   */
  private static final byte LONG_VALUE_TYPE_OTHER_PAGES = (byte) 0x00;

  private static final Pattern GUID_PATTERN = Pattern.compile("\\s*[{]([\\p{XDigit}]{8})-([\\p{XDigit}]{4})-([\\p{XDigit}]{4})-([\\p{XDigit}]{4})-([\\p{XDigit}]{12})[}]\\s*");

  /** For text columns, whether or not they are compressed */ 
  private boolean _compressedUnicode = false;
  /** Whether or not the column is of variable length */
  private boolean _variableLength;
  /** Numeric precision */
  private byte _precision;
  /** Numeric scale */
  private byte _scale;
  /** Data type */
  private DataType _type;
  /** Format that the containing database is in */
  private JetFormat _format;
  /** Used to read in LVAL pages */
  private PageChannel _pageChannel;
  /** Maximum column length */
  private short _columnLength;
  /** 0-based column number */
  private short _columnNumber;
  /** Column name */
  private String _name;
  /** the offset of the fixed data in the row */
  private int _fixedDataOffset;
  /** the index of the variable length data in the var len offset table */
  private int _varLenTableIndex;
  
  public Column() {
    this(JetFormat.VERSION_4);
  }
  
  public Column(JetFormat format) {
    _format = format;
  }
  
  /**
   * Read a column definition in from a buffer
   * @param buffer Buffer containing column definition
   * @param offset Offset in the buffer at which the column definition starts
   * @param format Format that the containing database is in
   */
  public Column(ByteBuffer buffer, int offset, PageChannel pageChannel, JetFormat format)
  throws IOException
  {
    _pageChannel = pageChannel;
    _format = format;
    setType(DataType.fromByte(buffer.get(offset + format.OFFSET_COLUMN_TYPE)));
    _columnNumber = buffer.getShort(offset + format.OFFSET_COLUMN_NUMBER);
    _columnLength = buffer.getShort(offset + format.OFFSET_COLUMN_LENGTH);
    if (_type.getHasScalePrecision()) {
      _precision = buffer.get(offset + format.OFFSET_COLUMN_PRECISION);
      _scale = buffer.get(offset + format.OFFSET_COLUMN_SCALE);
    }
    _variableLength = ((buffer.get(offset + format.OFFSET_COLUMN_VARIABLE)
        & 1) != 1);
    _compressedUnicode = ((buffer.get(offset +
        format.OFFSET_COLUMN_COMPRESSED_UNICODE) & 1) == 1);

    if(_variableLength) {
      _varLenTableIndex = buffer.getShort(offset + format.OFFSET_COLUMN_VARIABLE_TABLE_INDEX);
    } else {
      _fixedDataOffset = buffer.getShort(offset + format.OFFSET_COLUMN_FIXED_DATA_OFFSET);
    }
  }
  
  public String getName() {
    return _name;
  }
  public void setName(String name) {
    _name = name;
  }
  
  public boolean isVariableLength() {
    return _variableLength;
  }
  public void setVariableLength(boolean variableLength) {
    _variableLength = variableLength;
  }
  
  public short getColumnNumber() {
    return _columnNumber;
  }
  
  /**
   * Also sets the length and the variable length flag, inferred from the
   * type.  For types with scale/precision, sets the scale and precision to
   * default values.
   */
  public void setType(DataType type) {
    _type = type;
    if(!type.isVariableLength()) {
      setLength((short)type.getFixedSize());
    } else if(!type.isLongValue()) {
      setLength((short)type.getDefaultSize());
    }
    setVariableLength(type.isVariableLength());
    if(type.getHasScalePrecision()) {
      setScale((byte)type.getDefaultScale());
      setPrecision((byte)type.getDefaultPrecision());
    }
  }
  public DataType getType() {
    return _type;
  }
  
  public int getSQLType() throws SQLException {
    return _type.getSQLType();
  }
  
  public void setSQLType(int type) throws SQLException {
    setSQLType(type, 0);
  }
  
  public void setSQLType(int type, int lengthInUnits) throws SQLException {
    setType(DataType.fromSQLType(type, lengthInUnits));
  }
  
  public boolean isCompressedUnicode() {
    return _compressedUnicode;
  }

  public byte getPrecision() {
    return _precision;
  }
  
  public void setPrecision(byte newPrecision) {
    _precision = newPrecision;
  }
  
  public byte getScale() {
    return _scale;
  }

  public void setScale(byte newScale) {
    _scale = newScale;
  }
  
  public void setLength(short length) {
    _columnLength = length;
  }
  public short getLength() {
    return _columnLength;
  }

  public void setLengthInUnits(short unitLength) {
    setLength((short)(getType().getUnitSize() * unitLength));
  }
  public short getLengthInUnits() {
    return (short)(getLength() / getType().getUnitSize());
  }
  
  public int getVarLenTableIndex() {
    return _varLenTableIndex;
  }

  public int getFixedDataOffset() {
    return _fixedDataOffset;
  }

  /**
   * Checks that this column definition is valid.
   *
   * @throw IllegalArgumentException if this column definition is invalid.
   */
  public void validate() {
    if(getType() == null) {
      throw new IllegalArgumentException("must have type");
    }
    if((getName() == null) || (getName().trim().length() == 0)) {
      throw new IllegalArgumentException("must have valid name");
    }
    if(isVariableLength() != getType().isVariableLength()) {
      throw new IllegalArgumentException("invalid variable length setting");
    }

    if(!isVariableLength()) {
      if(getLength() != getType().getFixedSize()) {
        throw new IllegalArgumentException("invalid fixed length size");
      }
    } else if(!getType().isLongValue()) {
      if(!getType().isValidSize(getLength())) {
        throw new IllegalArgumentException("var length out of range");
      }
    }

    if(getType().getHasScalePrecision()) {
      if(!getType().isValidScale(getScale())) {
        throw new IllegalArgumentException(
            "Scale must be from " + getType().getMinScale() + " to " +
            getType().getMaxScale() + " inclusive");
      }
      if(!getType().isValidPrecision(getPrecision())) {
        throw new IllegalArgumentException(
            "Precision must be from " + getType().getMinPrecision() + " to " +
            getType().getMaxPrecision() + " inclusive");
      }
    }
  }
  
  /**
   * Deserialize a raw byte value for this column into an Object
   * @param data The raw byte value
   * @return The deserialized Object
   */
  public Object read(byte[] data) throws IOException {
    return read(data, ByteOrder.LITTLE_ENDIAN);
  }
  
  /**
   * Deserialize a raw byte value for this column into an Object
   * @param data The raw byte value
   * @param order Byte order in which the raw value is stored
   * @return The deserialized Object
   */  
  public Object read(byte[] data, ByteOrder order) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(data);
    buffer.order(order);
    if (_type == DataType.BOOLEAN) {
      throw new IOException("Tried to read a boolean from data instead of null mask.");
    } else if (_type == DataType.BYTE) {
      return new Byte(buffer.get());
    } else if (_type == DataType.INT) {
      return new Short(buffer.getShort());
    } else if (_type == DataType.LONG) {
      return new Integer(buffer.getInt());
    } else if (_type == DataType.DOUBLE) {
      return new Double(buffer.getDouble());
    } else if (_type == DataType.FLOAT) {
      return new Float(buffer.getFloat());
    } else if (_type == DataType.SHORT_DATE_TIME) {
      return readDateValue(buffer);
    } else if (_type == DataType.BINARY) {
      return data;
    } else if (_type == DataType.TEXT) {
      return decodeTextValue(data);
    } else if (_type == DataType.MONEY) {
      return readCurrencyValue(buffer);
    } else if (_type == DataType.OLE) {
      if (data.length > 0) {
        return readLongValue(data);
      } else {
        return null;
      }
    } else if (_type == DataType.MEMO) {
      if (data.length > 0) {
        return readLongStringValue(data);
      } else {
        return null;
      }
    } else if (_type == DataType.NUMERIC) {
      return readNumericValue(buffer);
    } else if (_type == DataType.GUID) {
      return readGUIDValue(buffer);
    } else if (_type == DataType.UNKNOWN_0D) {
      return null;
    } else {
      throw new IOException("Unrecognized data type: " + _type);
    }
  }

  /**
   * @param lvalDefinition Column value that points to an LVAL record
   * @param outType optional 1 element array for returning the
   *                <code>LONG_VALUE_TYPE_*</code>
   * @return The LVAL data
   */
  private byte[] readLongValue(byte[] lvalDefinition)
    throws IOException
  {
    ByteBuffer def = ByteBuffer.wrap(lvalDefinition);
    def.order(ByteOrder.LITTLE_ENDIAN);
    int length = ByteUtil.get3ByteInt(def);
    // bail out gracefully here as we don't understand the format
    if (length < 0)
    {
       return null;
    }
    byte[] rtn = new byte[length];
    byte type = def.get();

    if(type == LONG_VALUE_TYPE_THIS_PAGE) {

      // inline long value
      def.getInt();  //Skip over lval_dp
      def.getInt();  //Skip over unknown
      def.get(rtn);

    } else {

      // long value on other page(s)
      if (lvalDefinition.length != _format.SIZE_LONG_VALUE_DEF) {
        throw new IOException("Expected " + _format.SIZE_LONG_VALUE_DEF +
                              " bytes in long value definition, but found " +
                              lvalDefinition.length);
      }

      byte rowNum = def.get();
      int pageNum = ByteUtil.get3ByteInt(def, def.position());
      ByteBuffer lvalPage = _pageChannel.createPageBuffer();
      
      switch (type) {
      case LONG_VALUE_TYPE_OTHER_PAGE:
        {
          _pageChannel.readPage(lvalPage, pageNum);

          short rowStart = Table.findRowStart(lvalPage, rowNum, _format);
          short rowEnd = Table.findRowEnd(lvalPage, rowNum, _format);

          if((rowEnd - rowStart) != length) {
            throw new IOException("Unexpected lval row length");
          }
        
          lvalPage.position(rowStart);
          lvalPage.get(rtn);
        }
        break;
        
      case LONG_VALUE_TYPE_OTHER_PAGES:

        ByteBuffer rtnBuf = ByteBuffer.wrap(rtn);
        int remainingLen = length;
        while(remainingLen > 0) {
          lvalPage.clear();
          _pageChannel.readPage(lvalPage, pageNum);

          short rowStart = Table.findRowStart(lvalPage, rowNum, _format);
          short rowEnd = Table.findRowEnd(lvalPage, rowNum, _format);
          
          // read next page information
          lvalPage.position(rowStart);
          rowNum = lvalPage.get();
          pageNum = ByteUtil.get3ByteInt(lvalPage);

          // update rowEnd and remainingLen based on chunkLength
          int chunkLength = (rowEnd - rowStart) - 4;
          if(chunkLength > remainingLen) {
            rowEnd = (short)(rowEnd - (chunkLength - remainingLen));
            chunkLength = remainingLen;
          }
          remainingLen -= chunkLength;
          
          lvalPage.limit(rowEnd);
          rtnBuf.put(lvalPage);
        }
        
        break;
        
      default:
        throw new IOException("Unrecognized long value type: " + type);
      }
    }
    
    return rtn;
  }
  
  /**
   * @param lvalDefinition Column value that points to an LVAL record
   * @return The LVAL data
   */
  private String readLongStringValue(byte[] lvalDefinition)
    throws IOException
  {
    byte[] binData = readLongValue(lvalDefinition);
    if(binData == null) {
      return null;
    }
    return decodeTextValue(binData);
  }

  /**
   * Decodes "Currency" values.
   * 
   * @param valueBytes Column value that points to currency data
   * @return BigDecimal representing the monetary value
   * @throws IOException if the value cannot be parsed 
   */
  private BigDecimal readCurrencyValue(ByteBuffer buffer)
    throws IOException
  {
    if(buffer.remaining() != 8) {
      throw new IOException("Invalid money value.");
    }
    
    return new BigDecimal(BigInteger.valueOf(buffer.getLong(0)), 4);
  }

  /**
   * Writes "Currency" values.
   */
  private void writeCurrencyValue(ByteBuffer buffer, Object value)
    throws IOException
  {
    try {
      BigDecimal decVal = toBigDecimal(value);

      // adjust scale (will cause the an ArithmeticException if number has too
      // many decimal places)
      decVal = decVal.setScale(4);
    
      // now, remove scale and convert to long (this will throw if the value is
      // too big)
      buffer.putLong(decVal.movePointRight(4).longValueExact());
    } catch(ArithmeticException e) {
      throw (IOException)
        new IOException("Currency value out of range").initCause(e);
    }
  }

  /**
   * Decodes a NUMERIC field.
   */
  private BigDecimal readNumericValue(ByteBuffer buffer)
  {
    boolean negate = (buffer.get() != 0);

    byte[] tmpArr = new byte[16];
    buffer.get(tmpArr);

    if(buffer.order() != ByteOrder.BIG_ENDIAN) {
      fixNumericByteOrder(tmpArr);
    }

    BigInteger intVal = new BigInteger(tmpArr);
    if(negate) {
      intVal = intVal.negate();
    }
    return new BigDecimal(intVal, getScale());
  }

  /**
   * Writes a numeric value.
   */
  private void writeNumericValue(ByteBuffer buffer, Object value)
    throws IOException
  {
    try {
      BigDecimal decVal = toBigDecimal(value);

      boolean negative = (decVal.compareTo(BigDecimal.ZERO) < 0);
      if(negative) {
        decVal = decVal.negate();
      }

      // write sign byte
      buffer.put(negative ? (byte)0x80 : (byte)0);

      // adjust scale according to this column type (will cause the an
      // ArithmeticException if number has too many decimal places)
      decVal = decVal.setScale(getScale());

      // check precision
      if(decVal.precision() > getPrecision()) {
        throw new IOException("Numeric value is too big for specified precision "
                              + getPrecision() + ": " + decVal);
      }
    
      // convert to unscaled BigInteger, big-endian bytes
      byte[] intValBytes = decVal.unscaledValue().toByteArray();
      if(intValBytes.length > 16) {
        throw new IOException("Too many bytes for valid BigInteger?");
      }
      if(intValBytes.length < 16) {
        byte[] tmpBytes = new byte[16];
        System.arraycopy(intValBytes, 0, tmpBytes, (16 - intValBytes.length),
                         intValBytes.length);
        intValBytes = tmpBytes;
      }
      if(buffer.order() != ByteOrder.BIG_ENDIAN) {
        fixNumericByteOrder(intValBytes);
      }
      buffer.put(intValBytes);
    } catch(ArithmeticException e) {
      throw (IOException)
        new IOException("Numeric value out of range").initCause(e);
    }
  }

  /**
   * Decodes a date value.
   */
  private Date readDateValue(ByteBuffer buffer)
  {
    // seems access stores dates in the local timezone.  guess you just hope
    // you read it in the same timezone in which it was written!
    double dval = buffer.getDouble();
    dval *= MILLISECONDS_PER_DAY;
    dval -= (DAYS_BETWEEN_EPOCH_AND_1900 * MILLISECONDS_PER_DAY);
    long time = (long)dval;
    TimeZone tz = TimeZone.getDefault();
    Date date = new Date(time - tz.getRawOffset());
    if (tz.inDaylightTime(date))
    {
      date = new Date(date.getTime() - tz.getDSTSavings());
    }
    return date;
  }

  /**
   * Writes a date value.
   */
  private void writeDateValue(ByteBuffer buffer, Object value)
  {
    if(value == null) {
      buffer.putDouble(0d);
    } else {
      // seems access stores dates in the local timezone.  guess you just
      // hope you read it in the same timezone in which it was written!
      Calendar cal = Calendar.getInstance();
      cal.setTime((Date) value);
      long ms = cal.getTimeInMillis();
      ms += (long) TimeZone.getDefault().getOffset(ms);
      buffer.putDouble((double) ms / MILLISECONDS_PER_DAY +
                       DAYS_BETWEEN_EPOCH_AND_1900);
    }
  }

  /**
   * Decodes a GUID value.
   */
  private String readGUIDValue(ByteBuffer buffer)
  {
    StringBuffer sb = new StringBuffer(22);
    sb.append("{");
    sb.append(ByteUtil.toHexString(buffer, 0, 4,
                                   false));
    sb.append("-");
    sb.append(ByteUtil.toHexString(buffer, 4, 2,
                                   false));
    sb.append("-");
    sb.append(ByteUtil.toHexString(buffer, 6, 2,
                                   false));
    sb.append("-");
    sb.append(ByteUtil.toHexString(buffer, 8, 2,
                                   false));
    sb.append("-");
    sb.append(ByteUtil.toHexString(buffer, 10, 6,
                                   false));
    sb.append("}");
    return (sb.toString());
  }

  /**
   * Writes a GUID value.
   */
  private void writeGUIDValue(ByteBuffer buffer, Object value)
    throws IOException
  {
    Matcher m = GUID_PATTERN.matcher(toCharSequence(value));
    if(m.matches()) {
      ByteUtil.writeHexString(buffer, m.group(1));
      ByteUtil.writeHexString(buffer, m.group(2));
      ByteUtil.writeHexString(buffer, m.group(3));
      ByteUtil.writeHexString(buffer, m.group(4));
      ByteUtil.writeHexString(buffer, m.group(5));
    } else {
      throw new IOException("Invalid GUID: " + value);
    }
  }
  
  /**
   * Write an LVAL column into a ByteBuffer inline if it fits, otherwise in
   * other data page(s).
   * @param value Value of the LVAL column
   * @return A buffer containing the LVAL definition and (possibly) the column
   *         value (unless written to other pages)
   */
  public ByteBuffer writeLongValue(byte[] value,
                                   int remainingRowLength) throws IOException
  {
    if(value.length > getType().getMaxSize()) {
      throw new IOException("value too big for column");
    }

    // determine which type to write
    byte type = 0;
    int lvalDefLen = _format.SIZE_LONG_VALUE_DEF;
    if((_format.SIZE_LONG_VALUE_DEF + value.length) <= remainingRowLength) {
      type = LONG_VALUE_TYPE_THIS_PAGE;
      lvalDefLen += value.length;
    } else if(Table.getRowSpaceUsage(value.length, _format) <=
              _format.MAX_ROW_SIZE)
    {
      type = LONG_VALUE_TYPE_OTHER_PAGE;
    } else {
      type = LONG_VALUE_TYPE_OTHER_PAGES;
    }

    ByteBuffer def = ByteBuffer.allocate(lvalDefLen);
    def.order(ByteOrder.LITTLE_ENDIAN);
    ByteUtil.put3ByteInt(def, value.length);
    def.put(type);

    if(type == LONG_VALUE_TYPE_THIS_PAGE) {
      // write long value inline
      def.putInt(0);
      def.putInt(0);  //Unknown
      def.put(value);
    } else {
      
      int firstLvalPageNum = PageChannel.INVALID_PAGE_NUMBER;
      byte firstLvalRow = 0;

      ByteBuffer lvalPage = _pageChannel.createPageBuffer();
      
      // write other page(s)
      switch(type) {
      case LONG_VALUE_TYPE_OTHER_PAGE:
        writeLongValueHeader(lvalPage);
        firstLvalRow = (byte)Table.addDataPageRow(lvalPage,
                                                  value.length,
                                                  _format);
        lvalPage.put(value);
        firstLvalPageNum = _pageChannel.writeNewPage(lvalPage);
        break;

      case LONG_VALUE_TYPE_OTHER_PAGES:

        ByteBuffer buffer = ByteBuffer.wrap(value);
        int remainingLen = buffer.remaining();
        buffer.limit(0);
        int lvalPageNum = _pageChannel.allocateNewPage();
        byte lvalRow = 0;
        int nextLvalPageNum = 0;
        while(remainingLen > 0) {
          lvalPage.clear();
          writeLongValueHeader(lvalPage);

          // figure out how much we will put in this page
          int chunkLength = Math.min(_format.MAX_ROW_SIZE - 4,
                                     remainingLen);
          nextLvalPageNum = ((chunkLength < remainingLen) ?
                             _pageChannel.allocateNewPage() : 0);

          // add row to this page
          lvalRow = (byte)Table.addDataPageRow(lvalPage, chunkLength + 4,
                                               _format);
          
          // write next page info (we'll always be writing into row 0 for
          // newly created pages)
          lvalPage.put((byte)0); // row number
          ByteUtil.put3ByteInt(lvalPage, nextLvalPageNum); // page number

          // write this page's chunk of data
          buffer.limit(buffer.limit() + chunkLength);
          lvalPage.put(buffer);
          remainingLen -= chunkLength;

          // write new page to database
          _pageChannel.writePage(lvalPage, lvalPageNum);
          
          // hang onto first page info
          if(firstLvalPageNum == PageChannel.INVALID_PAGE_NUMBER) {
            firstLvalPageNum = lvalPageNum;
            firstLvalRow = lvalRow;
          }

          // move to next page
          lvalPageNum = nextLvalPageNum;
        }
        break;

      default:
        throw new IOException("Unrecognized long value type: " + type);
      }

      // update def
      def.put(firstLvalRow);
      ByteUtil.put3ByteInt(def, firstLvalPageNum);
      def.putInt(0);  //Unknown
      
    }
      
    def.flip();
    return def;
  }

  /**
   * Writes the header info for a long value page.
   */
  private void writeLongValueHeader(ByteBuffer lvalPage)
  {
    lvalPage.put(PageTypes.DATA); //Page type
    lvalPage.put((byte) 1); //Unknown
    lvalPage.putShort((short) (_format.PAGE_SIZE -
                               _format.OFFSET_ROW_START)); //Free space
    lvalPage.put((byte) 'L');
    lvalPage.put((byte) 'V');
    lvalPage.put((byte) 'A');
    lvalPage.put((byte) 'L');
    lvalPage.putShort((short)0); // num rows in page
    lvalPage.putInt(0); //unknown
  }
  
  /**
   * Serialize an Object into a raw byte value for this column in little endian order
   * @param obj Object to serialize
   * @return A buffer containing the bytes
   */
  public ByteBuffer write(Object obj, int remainingRowLength)
    throws IOException
  {
    return write(obj, remainingRowLength, ByteOrder.LITTLE_ENDIAN);
  }
  
  /**
   * Serialize an Object into a raw byte value for this column
   * @param obj Object to serialize
   * @param order Order in which to serialize
   * @return A buffer containing the bytes
   */
  public ByteBuffer write(Object obj, int remainingRowLength, ByteOrder order)
    throws IOException
  {
    if(!isVariableLength()) {
      return writeFixedLengthField(obj, order);
    }
      
    DataType t = getType();
    // var length column
    if(!t.isLongValue())
    {

      // this is an "inline" var length field
      if (t == DataType.NUMERIC)
      {
        // don't ask me why numerics are "var length" columns...
        ByteBuffer buffer = ByteBuffer.allocate(getLength());
        buffer.order(order);
        writeNumericValue(buffer, obj);
        buffer.flip();
        return buffer;
		}
      else if (t == DataType.TEXT)
      {
        CharSequence text = toCharSequence(obj);
        int maxChars = getLength() / 2;
        if (text.length() > maxChars) {
          throw new IOException("Text is too big for column");
        }
        byte[] encodedData = encodeUncompressedText(text).array();
        obj = encodedData;
      }
      else if (t == DataType.BINARY)
        // should already be "encoded"
        ;
      else
        throw new RuntimeException("unexpected inline var length type: "+t);

      ByteBuffer buffer = ByteBuffer.wrap((byte[])obj);
      buffer.order(order);
      return buffer;
    }

    // var length, long value column
    if (t == DataType.OLE)
      // should already be "encoded"
      ;
    else if (t == DataType.MEMO)
      obj = encodeUncompressedText(toCharSequence(obj)).array();
    else
      throw new RuntimeException("unexpected var length, long value type: "+t);

    // create long value buffer
    return writeLongValue((byte[]) obj, remainingRowLength);
  }

  /**
   * Serialize an Object into a raw byte value for this column
   * @param obj Object to serialize
   * @param order Order in which to serialize
   * @return A buffer containing the bytes
   */
  public ByteBuffer writeFixedLengthField(Object obj, ByteOrder order)
    throws IOException
  {
    int size = getType().getFixedSize();

    // create buffer for data
    ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.order(order);

    obj = booleanToInteger(obj);
	 DataType t = getType();
    if (t == DataType.BOOLEAN)
      ;
    else if (t == DataType.BYTE)
      buffer.put(obj != null ? ((Number) obj).byteValue() : (byte) 0);
    else if (t == DataType.INT)
      buffer.putShort(obj != null ? ((Number) obj).shortValue() : (short) 0);
    else if (t == DataType.LONG)
      buffer.putInt(obj != null ? ((Number) obj).intValue() : 0);
    else if (t == DataType.DOUBLE)
      buffer.putDouble(obj != null ? ((Number) obj).doubleValue() : (double) 0);
    else if (t == DataType.FLOAT)
      buffer.putFloat(obj != null ? ((Number) obj).floatValue() : (float) 0);
    else if (t == DataType.SHORT_DATE_TIME)
      writeDateValue(buffer, obj);
    else if (t == DataType.MONEY)
      writeCurrencyValue(buffer, obj);
    else if (t == DataType.GUID)
      writeGUIDValue(buffer, obj);
    else
      throw new IOException("Unsupported data type: " + getType());

    buffer.flip();
    return buffer;
  }
  
  /**
   * Decodes a compressed or uncompressed text value.
   */
  private String decodeTextValue(byte[] data)
    throws IOException
  {
    try {

      // see if data is compressed.  the 0xFF, 0xFE sequence indicates that
      // compression is used (sort of, see algorithm below)
      boolean isCompressed = ((data.length > 1) &&
                              (data[0] == (byte)0xFF) &&
                              (data[1] == (byte)0xFE));
      if(isCompressed) {

        Expand expander = new Expand();
        
        // this is a whacky compression combo that switches back and forth
        // between compressed/uncompressed using a 0x00 byte (starting in
        // compressed mode)
        StringBuffer textBuf = new StringBuffer(data.length);
        // start after two bytes indicating compression use
        int dataStart = 2;
        int dataEnd = dataStart;
        boolean inCompressedMode = true;
        while(dataEnd < data.length) {
          if(data[dataEnd] == (byte)0x00) {

            // handle current segment
            decodeTextSegment(data, dataStart, dataEnd, inCompressedMode,
                              expander, textBuf);
            inCompressedMode = !inCompressedMode;
            ++dataEnd;
            dataStart = dataEnd;
            
          } else {
            ++dataEnd;
          }
        }
        // handle last segment
        decodeTextSegment(data, dataStart, dataEnd, inCompressedMode,
                          expander, textBuf);

        return textBuf.toString();
        
      } else {
        return decodeUncompressedText(data);
      }
      
    } catch (IllegalInputException e) {
      throw (IOException)
        new IOException("Can't expand text column").initCause(e);
    } catch (EndOfInputException e) {
      throw (IOException)
        new IOException("Can't expand text column").initCause(e);
    }
  }

  /**
   * Decodes a segnment of a text value into the given buffer according to the
   * given status of the segment (compressed/uncompressed).
   */
  private void decodeTextSegment(byte[] data, int dataStart, int dataEnd,
                                 boolean inCompressedMode, Expand expander,
                                 StringBuffer textBuf)
    throws IllegalInputException, EndOfInputException
  {
    if(dataEnd <= dataStart) {
      // no data
      return;
    }
    int dataLength = dataEnd - dataStart;
    if(inCompressedMode) {
      // handle compressed data
      byte[] tmpData = new byte[dataLength];
      System.arraycopy(data, dataStart, tmpData, 0, dataLength);
      expander.reset();
      textBuf.append(expander.expand(tmpData));
    } else {
      // handle uncompressed data
      textBuf.append(decodeUncompressedText(data, dataStart, dataLength));
    }
  }

  /**
   * @param text Text to encode
   * @return A buffer with the text encoded
   */
  private ByteBuffer encodeUncompressedText(CharSequence text) {
    return _format.CHARSET.encode(CharBuffer.wrap(text));
  }

  /**
   * @param textBytes bytes of text to decode
   * @return the decoded string
   */
  private String decodeUncompressedText(byte[] textBytes) {
    return decodeUncompressedText(textBytes, 0, textBytes.length);
  }
  
  /**
   * @param textBytes bytes of text to decode
   * @return the decoded string
   */
  private String decodeUncompressedText(byte[] textBytes, int startPost,
                                            int length) {
    return _format.CHARSET.decode(ByteBuffer.wrap(textBytes, startPost,
                                                  length)).toString();
  }  
    
  public String toString() {
    StringBuffer rtn = new StringBuffer();
    rtn.append("\tName: " + _name);
    rtn.append("\n\tType: 0x" + Integer.toHexString((int)_type.getValue()));
    rtn.append("\n\tNumber: " + _columnNumber);
    rtn.append("\n\tLength: " + _columnLength);
    rtn.append("\n\tVariable length: " + _variableLength);
    rtn.append("\n\tCompressed Unicode: " + _compressedUnicode);
    rtn.append("\n\n");
    return rtn.toString();
  }
  
  public int compareTo(Object obj)
  {
    Column other = (Column)obj;
    if (_columnNumber > other.getColumnNumber()) {
      return 1;
    } else if (_columnNumber < other.getColumnNumber()) {
      return -1;
    } else {
      return 0;
    }
  }
  
  /**
   * @param columns A list of columns in a table definition
   * @return The number of variable length columns found in the list
   */
  public static short countVariableLength(List columns) {
    short rtn = 0;
    Iterator iter = columns.iterator();
    while (iter.hasNext()) {
      Column col = (Column) iter.next();
      if (col.isVariableLength()) {
        rtn++;
      }
    }
    return rtn;
  }

  /**
   * @return an appropriate BigDecimal representation of the given object.
   *         <code>null</code> is returned as 0 and Numbers are converted
   *         using their double representation.
   */
  private static BigDecimal toBigDecimal(Object value)
  {
    if(value == null) {
      return BigDecimal.ZERO;
    } else if(value instanceof BigDecimal) {
      return (BigDecimal)value;
    } else if(value instanceof BigInteger) {
      return new BigDecimal((BigInteger)value);
    } else if(value instanceof Number) {
      return new BigDecimal(((Number)value).doubleValue());
    } else {
      return new BigDecimal(value.toString());
    }
  }

  /**
   * @return an appropriate CharSequence representation of the given object.
   */
  public static CharSequence toCharSequence(Object value)
  {
    if(value == null) {
      return null;
    } else if(value instanceof CharSequence) {
      return (CharSequence)value;
    } else {
      return value.toString();
    }
  }

  /**
   * Interpret a boolean value (null == false)
   */
  public static boolean toBooleanValue(Object obj) {
    return ((obj != null) && ((Boolean)obj).booleanValue());
  }
  
  /**
   * Swaps the bytes of the given numeric in place.
   */
  private static void fixNumericByteOrder(byte[] bytes)
  {
    // fix endianness of each 4 byte segment
    for(int i = 0; i < 4; ++i) {
      int idx = i * 4;
      byte b = bytes[idx + 0];
      bytes[idx + 0] = bytes[idx + 3];
      bytes[idx + 3] = b;
      b = bytes[idx + 1];
      bytes[idx + 1] = bytes[idx + 2];
      bytes[idx + 2] = b;
    }
  }

  /**
   * Treat booleans as integers (C-style).
   */
  private Object booleanToInteger(Object obj) {
    if (obj instanceof Boolean) {
      obj = new Integer(((Boolean)obj).booleanValue() ? 1 : 0);
    }
    return obj;
  }

}
