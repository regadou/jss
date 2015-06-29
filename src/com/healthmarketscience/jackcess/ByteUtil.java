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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Byte manipulation and display utilities
 * @author Tim McCune
 */
public final class ByteUtil {
  
  private static final String[] HEX_CHARS = new String[] {
      "0", "1", "2", "3", "4", "5", "6", "7",
      "8", "9", "A", "B", "C", "D", "E", "F"};
      
  private ByteUtil() {}

  /**
   * Put an integer into the given buffer at the given offset as a 3-byte
   * integer.
   * @param buffer buffer into which to insert the int
   * @param val Int to convert
   */    
  public static void put3ByteInt(ByteBuffer buffer, int val)
  {
    int pos = buffer.position();
    put3ByteInt(buffer, val, pos);
    buffer.position(pos + 3);
  }
  
  /**
   * Put an integer into the given buffer at the given offset as a 3-byte
   * integer.
   * @param buffer buffer into which to insert the int
   * @param val Int to convert
   * @param offset offset at which to insert the int
   */    
  public static void put3ByteInt(ByteBuffer buffer, int val, int offset) {

    int offInc = 1;
    if(buffer.order() == ByteOrder.BIG_ENDIAN) {
      offInc = -1;
      offset += 2;
    }

    buffer.put(offset, (byte) (val & 0xFF));
    buffer.put(offset + (1 * offInc), (byte) ((val >>> 8) & 0xFF));
    buffer.put(offset + (2 * offInc), (byte) ((val >>> 16) & 0xFF));
  }

  /**
   * Read a 3 byte int from a buffer
   * @param buffer Buffer containing the bytes
   * @return The int
   */
  public static int get3ByteInt(ByteBuffer buffer) {
    return get3ByteInt(buffer, buffer.order());
  }

  /**
   * Read a 3 byte int from a buffer
   * @param buffer Buffer containing the bytes
   * @param order  the order of the bytes of the int
   * @return The int
   */
  public static int get3ByteInt(ByteBuffer buffer, ByteOrder order) {  
    int pos = buffer.position();
    int rtn = get3ByteInt(buffer, pos, order);
    buffer.position(pos + 3);
    return rtn;
  }

  /**
   * Read a 3 byte int from a buffer
   * @param buffer Buffer containing the bytes
   * @param offset Offset at which to start reading the int
   * @return The int
   */
  public static int get3ByteInt(ByteBuffer buffer, int offset) {
    return get3ByteInt(buffer, offset, buffer.order());
  }
  
  /**
   * Read a 3 byte int from a buffer
   * @param buffer Buffer containing the bytes
   * @param offset Offset at which to start reading the int
   * @param order  the order of the bytes of the int
   * @return The int
   */
  public static int get3ByteInt(ByteBuffer buffer, int offset,
                                ByteOrder order) {

    int offInc = 1;
    if(order == ByteOrder.BIG_ENDIAN) {
      offInc = -1;
      offset += 2;
    }
    
    int rtn = toUnsignedInt(buffer.get(offset));
    rtn += (toUnsignedInt(buffer.get(offset + (1 * offInc))) << 8);
    rtn += (toUnsignedInt(buffer.get(offset + (2 * offInc))) << 16);
    rtn &= 0xFFFFFF;
    return rtn;
  }

  /**
   * @return an int from the current position in the given buffer, read using
   *         the given ByteOrder
   */
  public static int getInt(ByteBuffer buffer, ByteOrder order) {
    int offset = buffer.position();
    int rtn = getInt(buffer, offset, order);
    buffer.position(offset + 4);
    return rtn;
  }
  
  /**
   * @return an int from the given position in the given buffer, read using
   *         the given ByteOrder
   */
  public static int getInt(ByteBuffer buffer, int offset, ByteOrder order) {
    ByteOrder origOrder = buffer.order();
    try {
      return buffer.order(order).getInt(offset);
    } finally {
      buffer.order(origOrder);
    }
  }
  
  /**
   * Writes an int at the current position in the given buffer, using the
   * given ByteOrder
   */
  public static void putInt(ByteBuffer buffer, int val, ByteOrder order) {
    int offset = buffer.position();
    putInt(buffer, val, offset, order);
    buffer.position(offset + 4);
  }
  
  /**
   * Writes an int at the given position in the given buffer, using the
   * given ByteOrder
   */
  public static void putInt(ByteBuffer buffer, int val, int offset,
                           ByteOrder order)
  {
    ByteOrder origOrder = buffer.order();
    try {
      buffer.order(order).putInt(offset, val);
    } finally {
      buffer.order(origOrder);
    }
  }
  
  /**
   * Convert a byte buffer to a hexadecimal string for display
   * @param buffer Buffer to display, starting at offset 0
   * @param size Number of bytes to read from the buffer
   * @return The display String
   */
  public static String toHexString(ByteBuffer buffer, int size) {
    return toHexString(buffer, 0, size);
  }
  
  /**
   * Convert a byte buffer to a hexadecimal string for display
   * @param buffer Buffer to display, starting at offset 0
   * @param offset Offset at which to start reading the buffer
   * @param size Number of bytes to read from the buffer
   * @return The display String
   */
  public static String toHexString(ByteBuffer buffer, int offset, int size) {
    return toHexString(buffer, offset, size, true);
  }    

  /**
   * Convert a byte buffer to a hexadecimal string for display
   * @param buffer Buffer to display, starting at offset 0
   * @param offset Offset at which to start reading the buffer
   * @param size Number of bytes to read from the buffer
   * @param formatted flag indicating if formatting is required
   * @return The display String
   */
  public static String toHexString(ByteBuffer buffer,
                                   int offset, int size, boolean formatted) {
    
    StringBuffer rtn = new StringBuffer();
    int position = buffer.position();
    buffer.position(offset);

    for (int i = 0; i < size; i++) {
      byte b = buffer.get();
      byte h = (byte) (b & 0xF0);
      h = (byte) (h >>> 4);
      h = (byte) (h & 0x0F);
      rtn.append(HEX_CHARS[(int) h]);
      h = (byte) (b & 0x0F);
      rtn.append(HEX_CHARS[(int) h]);

      if (formatted == true)
      {
        rtn.append(" ");

        if ((i + 1) % 4 == 0) {
          rtn.append(" ");
        }
        if ((i + 1) % 24 == 0) {
          rtn.append("\n");
        }
      }
    }

    buffer.position(position);
    return rtn.toString();
  }

  /**
   * Writes a sequence of hexidecimal values into the given buffer, where
   * every two characters represent one byte value.
   */
  public static void writeHexString(ByteBuffer buffer,
                                    String hexStr)
    throws IOException
  {
    char[] hexChars = hexStr.toCharArray();
    if((hexChars.length % 2) != 0) {
      throw new IOException("Hex string length must be even");
    }
    for(int i = 0; i < hexChars.length; i += 2) {
      String tmpStr = new String(hexChars, i, 2);
      buffer.put((byte)Long.parseLong(tmpStr, 16));
    }
  }

  /**
   * Writes a chunk of data to a file in pretty printed hexidecimal.
   */
  public static void toHexFile(
      String fileName,
      ByteBuffer buffer, 
      int offset, int size)
    throws IOException
  {
    PrintWriter writer = new PrintWriter(
        new FileWriter(fileName));
    try {
      writer.println(toHexString(buffer, offset, size));
    } finally {
      writer.close();
    }
  }

  /**
   * @return the byte value converted to an unsigned int value
   */
  public static int toUnsignedInt(byte b) { 
    return (int)b & 0xFF;
  }
  
}
