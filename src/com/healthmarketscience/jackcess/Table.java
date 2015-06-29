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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A single database table
 * @author Tim McCune
 */
public class Table
{

  private static final int INVALID_ROW_NUMBER = -1;
  
  private static final short OFFSET_MASK = (short)0x1FFF;

  private static final short DELETED_ROW_MASK = (short)0x8000;
  
  private static final short OVERFLOW_ROW_MASK = (short)0x4000;

  /** Table type code for system tables */
  public static final byte TYPE_SYSTEM = 0x53;
  /** Table type code for user tables */
  public static final byte TYPE_USER = 0x4e;
  
  /** State used for reading the table rows */
  private RowState _rowState;
  /** Type of the table (either TYPE_SYSTEM or TYPE_USER) */
  private byte _tableType;
  /** Number of the current row in a data page */
  private int _currentRowInPage = INVALID_ROW_NUMBER;
  /** Number of indexes on the table */
  private int _indexCount;
  /** Number of index slots for the table */
  private int _indexSlotCount;
  /** Number of rows in the table */
  private int _rowCount;
  /** page number of the definition of this table */
  private int _tableDefPageNumber;
  /** Number of rows left to be read on the current page */
  private short _rowsLeftOnPage = 0;
  /** max Number of columns in the table (includes previous deletions) */
  private short _maxColumnCount;
  /** max Number of variable columns in the table */
  private short _maxVarColumnCount;
  /** Number of variable columns in the table */
  private short _varColumnCount;
  /** Number of fixed columns in the table */
  private short _fixedColumnCount;
  /** Number of columns in the table */
  private short _columnCount;
  /** Format of the database that contains this table */
  private JetFormat _format;
  /** List of columns in this table */
  private List _columns = new ArrayList();
  /** List of indexes on this table */
  private List _indexes = new ArrayList();
  /** Used to read in pages */
  private PageChannel _pageChannel;
  /** Table name as stored in Database */
  private String _name;
  /** Usage map of pages that this table owns */
  private UsageMap _ownedPages;
  /** Usage map of pages that this table owns with free space on them */
  private UsageMap _freeSpacePages;
  
  /**
   * Only used by unit tests
   */
  Table() throws IOException {
    _pageChannel = new PageChannel(null, JetFormat.VERSION_4);
  }
  
  /**
   * @param tableBuffer Buffer to read the table with
   * @param pageChannel Page channel to get database pages from
   * @param format Format of the database that contains this table
   * @param pageNumber Page number of the table definition
	 * @param name Table name
   */
  protected Table(ByteBuffer tableBuffer, PageChannel pageChannel,
                  JetFormat format, int pageNumber, String name)
  throws IOException
  {
    _pageChannel = pageChannel;
    _format = format;
    _tableDefPageNumber = pageNumber;
    _name = name;
    int nextPage;
    ByteBuffer nextPageBuffer = null;
    nextPage = tableBuffer.getInt(_format.OFFSET_NEXT_TABLE_DEF_PAGE);
    while (nextPage != 0) {
      if (nextPageBuffer == null) {
        nextPageBuffer = _pageChannel.createPageBuffer();
      }
      _pageChannel.readPage(nextPageBuffer, nextPage);
      nextPage = nextPageBuffer.getInt(_format.OFFSET_NEXT_TABLE_DEF_PAGE);
      ByteBuffer newBuffer = ByteBuffer.allocate(tableBuffer.capacity() +
                                                 format.PAGE_SIZE - 8);
      newBuffer.order(ByteOrder.LITTLE_ENDIAN);
      newBuffer.put(tableBuffer);
      newBuffer.put(nextPageBuffer.array(), 8, format.PAGE_SIZE - 8);
      tableBuffer = newBuffer;
      tableBuffer.flip();
    }
    readTableDefinition(tableBuffer);
    tableBuffer = null;

    _rowState = new RowState(true);
  }

  /**
   * @return The name of the table
   */
  public String getName() {
    return _name;
  }
  
  /**
   * @return All of the columns in this table (unmodifiable List)
   */
  public List getColumns() {
    return Collections.unmodifiableList(_columns);
  }
  
  /**
   * Only called by unit tests
   */
  void setColumns(List columns) {
    _columns = columns;
    _maxVarColumnCount = Column.countVariableLength(_columns);
  }
  
  /**
   * @return All of the Indexes on this table (unmodifiable List)
   */
  public List getIndexes() {
    return Collections.unmodifiableList(_indexes);
  }

  /**
   * Only called by unit tests
   */
  int getIndexSlotCount() {
    return _indexSlotCount;
  }
  
  /**
   * After calling this method, getNextRow will return the first row in the
   * table
   */
  public void reset() {
    _rowsLeftOnPage = 0;
    _currentRowInPage = INVALID_ROW_NUMBER;
    _ownedPages.reset();
    _rowState.reset();
  }
  
  /**
   * Delete the current row (retrieved by a call to {@link #getNextRow}).
   */
  public void deleteCurrentRow() throws IOException {
    if (_currentRowInPage == INVALID_ROW_NUMBER) {
      throw new IllegalStateException("Must call getNextRow first");
    }

    // FIXME, want to make this static, but writeDataPage is not static, also, this may screw up other rowstates...

    // delete flag always gets set in the "root" page (even if overflow row)
    ByteBuffer rowBuffer = _rowState.getPage(_pageChannel);
    int index = getRowStartOffset(_currentRowInPage, _format);
    rowBuffer.putShort(index, (short)(rowBuffer.getShort(index)
                                      | DELETED_ROW_MASK | OVERFLOW_ROW_MASK));
    writeDataPage(rowBuffer, _rowState.getPageNumber());
    _rowState.setDeleted(true);
  }
  
  /**
   * @return The next row in this table (Column name -> Column value)
   */
  public Map getNextRow() throws IOException {
    return getNextRow(null);
  }

  /**
   * @param columnNames Only column names in this collection will be returned
   * @return The next row in this table (Column name -> Column value)
   */
  public Map getNextRow(Collection columnNames) 
    throws IOException
  {
    // find next row
    ByteBuffer rowBuffer = positionAtNextRow();
    if (rowBuffer == null) {
      return null;
    }

    return getRow(rowBuffer, getRowNullMask(rowBuffer), _columns,
                  columnNames);
  }
  
  /**
   * Reads a single column from the given row.
   */
  public static Object getRowSingleColumn(
      RowState rowState, int pageNumber, int rowNum,
      Column column, PageChannel pageChannel, JetFormat format)
    throws IOException
  {
    // set row state to correct page
    rowState.setPage(pageChannel, pageNumber);

    // position at correct row
    ByteBuffer rowBuffer = positionAtRow(rowState, rowNum, pageChannel,
                                         format);
    if(rowBuffer == null) {
      // note, row state will indicate that row was deleted
      return null;
    }
    
    return getRowColumn(rowBuffer, getRowNullMask(rowBuffer), column);
  }
 
  /**
   * Reads some columns from the given row.
   * @param columnNames Only column names in this collection will be returned
   */
  public static Map getRow(
      RowState rowState, int pageNumber, int rowNum,
      Collection columns, PageChannel pageChannel, JetFormat format,
      Collection columnNames)
    throws IOException
  {
    // set row state to correct page
    rowState.setPage(pageChannel, pageNumber);

    // position at correct row
    ByteBuffer rowBuffer = positionAtRow(rowState, rowNum, pageChannel,
                                         format);
    if(rowBuffer == null) {
      // note, row state will indicate that row was deleted
      return null;
    }

    return getRow(rowBuffer, getRowNullMask(rowBuffer), columns,
                  columnNames);
  }

  /**
   * Reads the row data from the given row buffer.  Leaves limit unchanged.
   */
  private static Map getRow(
      ByteBuffer rowBuffer,
      NullMask nullMask,
      Collection columns,
      Collection columnNames)
    throws IOException
  {
    Map rtn = new LinkedHashMap(columns.size());
    Object lst[] = columns.toArray();
    for(int i = 0; i < lst.length; i++)
    {
      Column column = (Column)(lst[i]);
      Object value = null;
      if((columnNames == null) || (columnNames.contains(column.getName())))
      {
        // Add the value to the row data
        rtn.put(column.getName(), getRowColumn(rowBuffer, nullMask, column));
      }
    }
    return rtn;
    
  }
  
  /**
   * Reads the column data from the given row buffer.  Leaves limit unchanged.
   */
  private static Object getRowColumn(ByteBuffer rowBuffer,
                                     NullMask nullMask,
                                     Column column)
    throws IOException
  {
    boolean isNull = nullMask.isNull(column.getColumnNumber());
    if(column.getType() == DataType.BOOLEAN) {
      return new Boolean(!isNull);  //Boolean values are stored in the null mask
    } else if(isNull) {
      // well, that's easy!
      return null;
    }

    // reset position to row start
    rowBuffer.reset();
    
    // locate the column data bytes
    int rowStart = rowBuffer.position();
    int colDataPos = 0;
    int colDataLen = 0;
    if(!column.isVariableLength()) {

      // read fixed length value (non-boolean at this point)
      int dataStart = rowStart + 2;
      colDataPos = dataStart + column.getFixedDataOffset();
      colDataLen = column.getType().getFixedSize();
      
    } else {

      // read var length value
      int varColumnOffsetPos =
        (rowBuffer.limit() - nullMask.byteSize() - 4) -
        (column.getVarLenTableIndex() * 2);

      short varDataStart = rowBuffer.getShort(varColumnOffsetPos);
      short varDataEnd = rowBuffer.getShort(varColumnOffsetPos - 2);
      colDataPos = rowStart + varDataStart;
      colDataLen = varDataEnd - varDataStart;
    }

    // grab the column data
    byte[] columnData = new byte[colDataLen];
    rowBuffer.position(colDataPos);
    rowBuffer.get(columnData);

    // parse the column data
    return column.read(columnData);
  }

  /**
   * Reads the null mask from the given row buffer.  Leaves limit unchanged.
   */
  private static NullMask getRowNullMask(ByteBuffer rowBuffer)
    throws IOException
  {
    // reset position to row start
    rowBuffer.reset();
    
    short columnCount = rowBuffer.getShort(); // Number of columns in this row
    
    // read null mask
    NullMask nullMask = new NullMask(columnCount);
    rowBuffer.position(rowBuffer.limit() - nullMask.byteSize());  //Null mask at end
    nullMask.read(rowBuffer);

    return nullMask;
  }
                                        
  /**
   * Position the buffer at the next row in the table
   * @return a ByteBuffer narrowed to the next row, or null if none
   */
  private ByteBuffer positionAtNextRow() throws IOException {

    // loop until we find the next valid row or run out of pages
    while(true) {

      if (_rowsLeftOnPage == 0) {

        // reset row number
        _currentRowInPage = INVALID_ROW_NUMBER;
        
        Integer nextPageNumber = _ownedPages.getNextPage();
        if (nextPageNumber == null) {
          //No more owned pages.  No more rows.
          return null;
        }

        // load new page
        ByteBuffer rowBuffer = _rowState.setPage(_pageChannel, nextPageNumber.intValue());
        if(rowBuffer.get() != PageTypes.DATA) {
          //Only interested in data pages
          continue;
        }

        _rowsLeftOnPage = rowBuffer.getShort(_format.OFFSET_NUM_ROWS_ON_DATA_PAGE);
        if(_rowsLeftOnPage == 0) {
          // no rows on this page?
          continue;
        }
        
      }

      // move to next row
      _currentRowInPage++;
      _rowsLeftOnPage--;

      ByteBuffer rowBuffer =
        positionAtRow(_rowState, _currentRowInPage, _pageChannel, _format);
      if(rowBuffer != null) {
        // we found a non-deleted row, return it
        return rowBuffer;
      }
    }
  }

  /**
   * Sets the position and limit in a new buffer using the given rowState
   * according to the given row number and row end, following overflow row
   * pointers as necessary.
   * 
   * @return a ByteBuffer narrowed to the next row, or null if row was deleted
   */
  private static ByteBuffer positionAtRow(RowState rowState, int rowNum,
                                          PageChannel pageChannel,
                                          JetFormat format)
    throws IOException
  {
    // reset row state
    rowState.reset();
    
    while(true) {
      ByteBuffer rowBuffer = rowState.getFinalPage(pageChannel);
    
      // note, we don't use findRowStart here cause we need the unmasked value
      short rowStart = rowBuffer.getShort(getRowStartOffset(rowNum, format));
      short rowEnd = findRowEnd(rowBuffer, rowNum, format);

      // note, if we are reading from an overflow page, the row will be marked
      // as deleted on that page, so ignore the deletedRow flag on overflow
      // pages
      boolean deletedRow =
        (((rowStart & DELETED_ROW_MASK) != 0) && !rowState.isOverflow());
      boolean overflowRow = ((rowStart & OVERFLOW_ROW_MASK) != 0);

      // now, strip flags from rowStart offset
      rowStart = (short)(rowStart & OFFSET_MASK);

      if (deletedRow) {
      
        // Deleted row.  Skip.
        rowState.setDeleted(true);
        return null;
      
      } else if (overflowRow) {

        if((rowEnd - rowStart) < 4) {
          throw new IOException("invalid overflow row info");
        }
      
        // Overflow page.  the "row" data in the current page points to another
        // page/row
        int overflowRowNum = rowBuffer.get(rowStart);
        int overflowPageNum = ByteUtil.get3ByteInt(rowBuffer, rowStart + 1);
        rowState.setOverflowPage(pageChannel, overflowPageNum);

        // reset row number and move to overflow page
        rowNum = overflowRowNum;
      
      } else {

        return PageChannel.narrowBuffer(rowBuffer, rowStart, rowEnd);
      }
    }    
  }

  
  /**
   * Calls <code>reset</code> on this table and returns a modifiable Iterator
   * which will iterate through all the rows of this table.  Use of the
   * Iterator follows the same restrictions as a call to
   * <code>getNextRow</code>.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator iterator()
  {
    return iterator(null);
  }
  
  /**
   * Calls <code>reset</code> on this table and returns a modifiable Iterator
   * which will iterate through all the rows of this table, returning only the
   * given columns.  Use of the Iterator follows the same restrictions as a
   * call to <code>getNextRow</code>.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator iterator(Collection columnNames)
  {
    return new RowIterator(columnNames);
  }
  
  /**
   * Read the table definition
   */
  private void readTableDefinition(ByteBuffer tableBuffer) throws IOException
  {
    _rowCount = tableBuffer.getInt(_format.OFFSET_NUM_ROWS);
    _tableType = tableBuffer.get(_format.OFFSET_TABLE_TYPE);
    _maxColumnCount = tableBuffer.getShort(_format.OFFSET_MAX_COLS);
    _maxVarColumnCount = tableBuffer.getShort(_format.OFFSET_NUM_VAR_COLS);
    _columnCount = tableBuffer.getShort(_format.OFFSET_NUM_COLS);
    _indexSlotCount = tableBuffer.getInt(_format.OFFSET_NUM_INDEX_SLOTS);
    _indexCount = tableBuffer.getInt(_format.OFFSET_NUM_INDEXES);
    
    byte rowNum = tableBuffer.get(_format.OFFSET_OWNED_PAGES);
    int pageNum = ByteUtil.get3ByteInt(tableBuffer, _format.OFFSET_OWNED_PAGES + 1);
    _ownedPages = UsageMap.read(_pageChannel, pageNum, rowNum, _format);
    rowNum = tableBuffer.get(_format.OFFSET_FREE_SPACE_PAGES);
    pageNum = ByteUtil.get3ByteInt(tableBuffer, _format.OFFSET_FREE_SPACE_PAGES + 1);
    _freeSpacePages = UsageMap.read(_pageChannel, pageNum, rowNum, _format);
    
    for (int i = 0; i < _indexCount; i++) {
      Index index = new Index(_tableDefPageNumber, _pageChannel, _format);
      _indexes.add(index);
      index.setRowCount(tableBuffer.getInt(_format.OFFSET_INDEX_DEF_BLOCK +
          i * _format.SIZE_INDEX_DEFINITION + 4));
    }
    
    int offset = _format.OFFSET_INDEX_DEF_BLOCK +
        _indexCount * _format.SIZE_INDEX_DEFINITION;
    Column column;
    for (int i = 0; i < _columnCount; i++) {
      column = new Column(tableBuffer,
          offset + i * _format.SIZE_COLUMN_HEADER, _pageChannel, _format);
      if(column.isVariableLength()) {
        _varColumnCount++;
      } else {
        _fixedColumnCount++;
      }
      _columns.add(column);
    }
    offset += _columnCount * _format.SIZE_COLUMN_HEADER;
    for (int i = 0; i < _columnCount; i++) {
      column = (Column) _columns.get(i);
      short nameLength = tableBuffer.getShort(offset);
      offset += 2;
      byte[] nameBytes = new byte[nameLength];
      tableBuffer.position(offset);
      tableBuffer.get(nameBytes, 0, (int) nameLength);
      column.setName(_format.CHARSET.decode(ByteBuffer.wrap(nameBytes)).toString());
      offset += nameLength;
    }
    Collections.sort(_columns);

    int idxOffset = tableBuffer.position();
    tableBuffer.position(idxOffset +
                     (_format.OFFSET_INDEX_NUMBER_BLOCK * _indexCount));

    // there are _indexSlotCount blocks here, we ignore any slot with an index
    // number greater than the number of actual indexes
    int curIndex = 0;
    for (int i = 0; i < _indexSlotCount; i++) {
      
      tableBuffer.getInt(); //Forward past Unknown
      int indexNumber = tableBuffer.getInt();
      tableBuffer.position(tableBuffer.position() + 15);
      byte indexType = tableBuffer.get();
      tableBuffer.position(tableBuffer.position() + 4);

      if(indexNumber < _indexCount) {
        Index index = (Index)(_indexes.get(curIndex++));
        index.setIndexNumber(indexNumber);
        index.setPrimaryKey(indexType == 1);
      }
    }

    // for each empty index slot, there is some weird sort of name
    for(int i = 0; i < (_indexSlotCount - _indexCount); ++i) {
      int skipBytes = tableBuffer.getShort();
      tableBuffer.position(tableBuffer.position() + skipBytes);
    }

    // read actual index names
    // FIXME, we still are not always getting the names matched correctly with
    // the index info, some weird indexing we are not figuring out yet
    for (int i = 0; i < _indexCount; i++) {
      byte[] nameBytes = new byte[tableBuffer.getShort()];
      tableBuffer.get(nameBytes);
      ((Index) _indexes.get(i)).setName(_format.CHARSET.decode(ByteBuffer.wrap(
          nameBytes)).toString());
    }
    int idxEndOffset = tableBuffer.position();
    
    Collections.sort(_indexes);

    // go back to index column info after sorting
    tableBuffer.position(idxOffset);
    for (int i = 0; i < _indexCount; i++) {
      tableBuffer.getInt(); //Forward past Unknown
      ((Index) _indexes.get(i)).read(tableBuffer, _columns);
    }

    // reset to end of index info
    tableBuffer.position(idxEndOffset);
  }

  /**
   * Writes the given page data to the given page number, clears any other
   * relevant buffers.
   */
  private void writeDataPage(ByteBuffer pageBuffer, int pageNumber)
    throws IOException
  {
    // write the page data
    _pageChannel.writePage(pageBuffer, pageNumber);

    // if the overflow buffer is this page, invalidate it
    _rowState.possiblyInvalidate(pageNumber, pageBuffer);
  }
  
  /**
   * Add a single row to this table and write it to disk
   */
  public void addRow(Object rows[]) throws IOException {
    addRows(Arrays.asList(rows));
  }

  
  /**
   * Add multiple rows to this table, only writing to disk after all
   * rows have been written, and every time a data page is filled.  This
   * is much more efficient than calling <code>addRow</code> multiple times.
   * @param rows List of Object[] row values
   */
  public void addRows(List rows) throws IOException {
    ByteBuffer dataPage = _pageChannel.createPageBuffer();
    ByteBuffer[] rowData = new ByteBuffer[rows.size()];
    Iterator iter = rows.iterator();
    for (int i = 0; iter.hasNext(); i++) {
      rowData[i] = createRow((Object[]) iter.next(), _format.MAX_ROW_SIZE);
      if (rowData[i].limit() > _format.MAX_ROW_SIZE) {
        throw new IOException("Row size " + rowData[i].limit() +
                              " is too large");
      }
    }
    
    int pageNumber = PageChannel.INVALID_PAGE_NUMBER;
    int rowSize;

    // find last data page (Not bothering to check other pages for free
    // space.)
    List pageNumbers = _ownedPages.getPageNumbers();
    for(ListIterator pageIter = pageNumbers.listIterator(
            pageNumbers.size()); pageIter.hasPrevious(); ) {
      Integer tmpPageNumber = (Integer)(pageIter.previous());
      _pageChannel.readPage(dataPage, tmpPageNumber.intValue());
      if(dataPage.get() == PageTypes.DATA) {
        // found last data page
        pageNumber = tmpPageNumber.intValue();
        break;
      }
    }

    if(pageNumber == PageChannel.INVALID_PAGE_NUMBER) {
      //No data pages exist.  Create a new one.
      pageNumber = newDataPage(dataPage);
    }
    
    for (int i = 0; i < rowData.length; i++) {
      rowSize = rowData[i].remaining();
      int rowSpaceUsage = getRowSpaceUsage(rowSize, _format);
      short freeSpaceInPage = dataPage.getShort(_format.OFFSET_FREE_SPACE);
      if (freeSpaceInPage < rowSpaceUsage) {

        //Last data page is full.  Create a new one.
        writeDataPage(dataPage, pageNumber);
        dataPage.clear();
        _freeSpacePages.removePageNumber(pageNumber);

        pageNumber = newDataPage(dataPage);
        freeSpaceInPage = dataPage.getShort(_format.OFFSET_FREE_SPACE);
      }

      // write out the row data
      int rowNum = addDataPageRow(dataPage, rowSize, _format);
      dataPage.put(rowData[i]);

      // update the indexes
      Iterator indIter = _indexes.iterator();
      while (indIter.hasNext()) {
        Index index = (Index) indIter.next();
        index.addRow((Object[]) rows.get(i), pageNumber, (byte) rowNum);
      }
    }
    writeDataPage(dataPage, pageNumber);
    
    //Update tdef page
    ByteBuffer tdefPage = _pageChannel.createPageBuffer();
    _pageChannel.readPage(tdefPage, _tableDefPageNumber);
    tdefPage.putInt(_format.OFFSET_NUM_ROWS, ++_rowCount);
    Iterator indIter = _indexes.iterator();
    for (int i = 0; i < _indexes.size(); i++) {
      tdefPage.putInt(_format.OFFSET_INDEX_DEF_BLOCK +
          i * _format.SIZE_INDEX_DEFINITION + 4, _rowCount);
      Index index = (Index) indIter.next();
      index.update();
    }
    _pageChannel.writePage(tdefPage, _tableDefPageNumber);
  }
  
  /**
   * Create a new data page
   * @return Page number of the new page
   */
  private int newDataPage(ByteBuffer dataPage) throws IOException {
    dataPage.put(PageTypes.DATA); //Page type
    dataPage.put((byte) 1); //Unknown
    dataPage.putShort((short)getRowSpaceUsage(_format.MAX_ROW_SIZE,
                                              _format)); //Free space in this page
    dataPage.putInt(_tableDefPageNumber); //Page pointer to table definition
    dataPage.putInt(0); //Unknown
    dataPage.putInt(0); //Number of records on this page
    int pageNumber = _pageChannel.writeNewPage(dataPage);
    _ownedPages.addPageNumber(pageNumber);
    _freeSpacePages.addPageNumber(pageNumber);
    return pageNumber;
  }
  
  /**
   * Serialize a row of Objects into a byte buffer
   */
  ByteBuffer createRow(Object[] rowArray, int maxRowSize) throws IOException {
    ByteBuffer buffer = _pageChannel.createPageBuffer();
    buffer.putShort((short) _columns.size());
    NullMask nullMask = new NullMask(_columns.size());
    Iterator iter;
    int index = 0;
    Column col;
    List row = new ArrayList(Arrays.asList(rowArray));
    
    //Append null for arrays that are too small
    for (int i = rowArray.length; i < _columnCount; i++) {
      row.add(null);
    }
    
    for (iter = _columns.iterator(); iter.hasNext() && index < row.size(); index++) {
      col = (Column) iter.next();
      if (!col.isVariableLength()) {
        //Fixed length column data comes first (remainingRowLength is ignored
        //when writing fixed length data
        buffer.put(col.write(row.get(index), 0));
      }
      if (col.getType() == DataType.BOOLEAN) {
        if(!Column.toBooleanValue(row.get(index))) {
          //Booleans are stored in the null mask
          nullMask.markNull(index);
        }
      } else if (row.get(index) == null) {
        nullMask.markNull(index);
      }
    }

    int varLengthCount = Column.countVariableLength(_columns);
    short[] varColumnOffsets = null;

    if(varLengthCount > 0) {
      // figure out how much space remains for var length data.  first,
      // account for already written space
      maxRowSize -= buffer.position();
      // now, account for trailer space
      maxRowSize -= (nullMask.byteSize() + 4 + (varLengthCount * 2));
    
      varColumnOffsets = new short[varLengthCount];
      index = 0;
      int varColumnOffsetsIndex = 0;
      //Now write out variable length column data
      for (iter = _columns.iterator(); iter.hasNext() && index < row.size();
           index++) {
        col = (Column) iter.next();
        short offset = (short) buffer.position();
        if (col.isVariableLength()) {
          if (row.get(index) != null) {
            ByteBuffer varDataBuf = col.write(row.get(index), maxRowSize);
            maxRowSize -= varDataBuf.remaining();
            buffer.put(varDataBuf);
          }
          varColumnOffsets[varColumnOffsetsIndex++] = offset;
        }
      }
    }

    // only need this info if this table contains any var length data
    if(_maxVarColumnCount > 0) {
      buffer.putShort((short) buffer.position()); //EOD marker
      //Now write out variable length offsets
      //Offsets are stored in reverse order
      for (int i = varLengthCount - 1; i >= 0; i--) {
        buffer.putShort(varColumnOffsets[i]);
      }
      buffer.putShort((short) varLengthCount);  //Number of var length columns
    }
    
    buffer.put(nullMask.wrap());  //Null mask
    buffer.limit(buffer.position());
    buffer.flip();
    return buffer;
  }

  public int getRowCount() {
    return _rowCount;
  }
  
  public String toString() {
    StringBuffer rtn = new StringBuffer();
    rtn.append("Type: " + _tableType);
		rtn.append("\nName: " + _name);
    rtn.append("\nRow count: " + _rowCount);
    rtn.append("\nColumn count: " + _columnCount);
    rtn.append("\nIndex count: " + _indexCount);
    rtn.append("\nColumns:\n");
    Iterator iter = _columns.iterator();
    while (iter.hasNext()) {
      rtn.append(iter.next().toString());
    }
    rtn.append("\nIndexes:\n");
    iter = _indexes.iterator();
    while (iter.hasNext()) {
      rtn.append(iter.next().toString());
    }
    rtn.append("\nOwned pages: " + _ownedPages + "\n");
    return rtn.toString();
  }
  
  /**
   * @return A simple String representation of the entire table in tab-delimited format
   */
  public String display() throws IOException {
    return display(Long.MAX_VALUE);
  }
  
  /**
   * @param limit Maximum number of rows to display
   * @return A simple String representation of the entire table in tab-delimited format
   */
  public String display(long limit) throws IOException {
    reset();
    StringBuffer rtn = new StringBuffer();
    Iterator iter = _columns.iterator();
    while (iter.hasNext()) {
      Column col = (Column) iter.next();
      rtn.append(col.getName());
      if (iter.hasNext()) {
        rtn.append("\t");
      }
    }
    rtn.append("\n");
    Map row;
    int rowCount = 0;
    while ((rowCount++ < limit) && (row = getNextRow()) != null) {
      iter = row.values().iterator();
      while (iter.hasNext()) {
        Object obj = iter.next();
        if (obj instanceof byte[]) {
          byte[] b = (byte[]) obj;
          rtn.append(ByteUtil.toHexString(ByteBuffer.wrap(b), b.length));
          //This block can be used to easily dump a binary column to a file
          /*java.io.File f = java.io.File.createTempFile("ole", ".bin");
            java.io.FileOutputStream out = new java.io.FileOutputStream(f);
            out.write(b);
            out.flush();
            out.close();*/
        } else {
          rtn.append(String.valueOf(obj));
        }
        if (iter.hasNext()) {
          rtn.append("\t");
        }
      }
      rtn.append("\n");
    }
    return rtn.toString();
  }

  /**
   * Updates free space and row info for a new row of the given size in the
   * given data page.  Positions the page for writing the row data.
   * @return the row number of the new row
   */
  public static int addDataPageRow(ByteBuffer dataPage,
                                   int rowSize,
                                   JetFormat format)
  {
    int rowSpaceUsage = getRowSpaceUsage(rowSize, format);
    
    // Decrease free space record.
    short freeSpaceInPage = dataPage.getShort(format.OFFSET_FREE_SPACE);
    dataPage.putShort(format.OFFSET_FREE_SPACE, (short) (freeSpaceInPage -
                                                         rowSpaceUsage));

    // Increment row count record.
    short rowCount = dataPage.getShort(format.OFFSET_NUM_ROWS_ON_DATA_PAGE);
    dataPage.putShort(format.OFFSET_NUM_ROWS_ON_DATA_PAGE,
                      (short) (rowCount + 1));

    // determine row position
    short rowLocation = findRowEnd(dataPage, rowCount, format);
    rowLocation -= rowSize;

    // write row position
    dataPage.putShort(getRowStartOffset(rowCount, format), rowLocation);

    // set position for row data
    dataPage.position(rowLocation);

    return rowCount;
  }
  
  public static short findRowStart(ByteBuffer buffer, int rowNum,
                                   JetFormat format)
  {
    return (short)(buffer.getShort(getRowStartOffset(rowNum, format))
                   & OFFSET_MASK);
  }

  public static int getRowStartOffset(int rowNum, JetFormat format)
  {
    return format.OFFSET_ROW_START + (format.SIZE_ROW_LOCATION * rowNum);
  }
  
  public static short findRowEnd(ByteBuffer buffer, int rowNum,
                                 JetFormat format)
  {
    return (short)((rowNum == 0) ?
                   format.PAGE_SIZE :
                   (buffer.getShort(getRowEndOffset(rowNum, format))
                    & OFFSET_MASK));
  }

  public static int getRowEndOffset(int rowNum, JetFormat format)
  {
    return format.OFFSET_ROW_START + (format.SIZE_ROW_LOCATION * (rowNum - 1));
  }

  public static int getRowSpaceUsage(int rowSize, JetFormat format)
  {
    return rowSize + format.SIZE_ROW_LOCATION;
  }
  
  /**
   * Row iterator for this table, supports modification.
   */
  private final class RowIterator implements Iterator
  {
    private Collection _columnNames;
    private Map _next;
    
    private RowIterator(Collection columnNames)
    {
      try {
        reset();
        _columnNames = columnNames;
        _next = getNextRow(_columnNames);
      } catch(IOException e) {
        throw new IllegalStateException(e);
      }
    }

    public boolean hasNext() { return _next != null; }

    public void remove() {
      try {
        deleteCurrentRow();
      } catch(IOException e) {
        throw new IllegalStateException(e);
      }        
    }
    
    public Object next() {
      if(!hasNext()) {
        throw new NoSuchElementException();
      }
      try {
        Map rtn = _next;
        _next = getNextRow(_columnNames);
        return rtn;
      } catch(IOException e) {
        throw new IllegalStateException(e);
      }
    }
    
  }

  /**
   * Maintains the state of reading a row of data.
   */
  public static class RowState
  {
    /** Buffer used for reading the row data pages */
    private TempPageHolder _rowBufferH;
    /** true if the current row is an overflow row */
    public boolean _overflow;
    /** true if the current row is a deleted row */
    public boolean _deleted;
    /** buffer used for reading overflow pages */
    public TempPageHolder _overflowRowBufferH =
      TempPageHolder.newHolder(false);
    /** the row buffer which contains the final data (after following any
        overflow pointers) */
    public ByteBuffer _finalRowBuffer;

    public RowState(boolean hardRowBuffer) {
      _rowBufferH = TempPageHolder.newHolder(hardRowBuffer);
    }
    
    public void reset() {
      _finalRowBuffer = null;
      _deleted = false;
      _overflow = false;
    }

    public int getPageNumber() {
      return _rowBufferH.getPageNumber();
    }

    public ByteBuffer getFinalPage(PageChannel pageChannel)
      throws IOException
    {
      if(_finalRowBuffer == null) {
        // (re)load current page
        _finalRowBuffer = getPage(pageChannel);
      }
      return _finalRowBuffer;
    }

    public void setDeleted(boolean deleted) {
      _deleted = deleted;
    }

    public boolean isDeleted() {
      return _deleted;
    }
    
    public boolean isOverflow() {
      return _overflow;
    }

    public void possiblyInvalidate(int modifiedPageNumber,
                                   ByteBuffer modifiedBuffer) {
      _rowBufferH.possiblyInvalidate(modifiedPageNumber,
                                     modifiedBuffer);
      _overflowRowBufferH.possiblyInvalidate(modifiedPageNumber,
                                             modifiedBuffer);
    }
    
    public ByteBuffer getPage(PageChannel pageChannel)
      throws IOException
    {
      return _rowBufferH.getPage(pageChannel);
    }

    public ByteBuffer setPage(PageChannel pageChannel, int pageNumber)
      throws IOException
    {
      reset();
      _finalRowBuffer = _rowBufferH.setPage(pageChannel, pageNumber);
      return _finalRowBuffer;
    }

    public ByteBuffer setOverflowPage(PageChannel pageChannel, int pageNumber)
      throws IOException
    {
      _overflow = true;
      _finalRowBuffer = _overflowRowBufferH.setPage(pageChannel, pageNumber);
      return _finalRowBuffer;
    }

  }
  
}
