package org.regadou.jmdb;

import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.sql.*;

public class RegResultSet implements ResultSet
{
	private RegStatement statement = null;
	private List fields = null, records = null;
	private int current = -1;
	private Object last = "";

	protected RegResultSet(RegStatement s, List r, List f)
	{
		statement = s;
		records = (r == null) ? new ArrayList() : r;
		fields = (f == null) ? new ArrayList() : f;
	}

	public int getColumn(String name) throws SQLException
	{
		if (fields == null)
			throw new SQLException("RecordSet is closed");
		else if (name == null || name.equals(""))
			throw new SQLException("Invalid column name");
		name = name.toLowerCase();
		int n = fields.size();
		for (int i = 0; i < n; i++)
		{
			Map fld = (Map)(fields.get(i));
			Object val = fld.get("column_name");
			if (val != null && val.toString().toLowerCase().equals(name))
				return i+1;
		}
		throw new SQLException("Invalid column name "+name);
	}

	public String getColumn(int no) throws SQLException
	{
		if (fields == null)
			throw new SQLException("RecordSet is closed");
		else if (no < 1 || no > fields.size())
			throw new SQLException("Invalid column number "+no);
		Object val = ((Map)(fields.get(no-1))).get("column_name");
		if (val == null)
			throw new SQLException("No name found for column "+no);
		else
			return val.toString();
	}

	public boolean absolute(int row)
	{
		if (records == null)
			return false;
		int n = records.size();
		if (row < 0)
			current = n - row;
		else if (row > n)
			current = n;
		else
			current = row - 1;
		return(current >= 0 && current < n);
	}

	public void afterLast()
	{
		current = (records == null) ? 0 : records.size();
	}

	public void beforeFirst()
	{
		current = -1;
	}

	public void cancelRowUpdates()
	{
	}

	public void clearWarnings()
	{
	}

	public void close()
	{
		if (statement != null)
			statement.removeresultset(this);
		records = fields = null;
		statement = null;
		current = -1;
		last = "";
	}

	public void deleteRow() throws SQLException
	{
		throw new SQLException("RecordSet is read only");
	}

	public int findColumn(String columnName)
	{
		return 0;
	}

	public boolean first()
	{
		return false;
	}

	public Array getArray(int i)
	{
		return null;
	}

	public Array getArray(String colName)
	{
		return null;
	}

	public InputStream getAsciiStream(int columnIndex)
	{
		return null;
	}

	public InputStream getAsciiStream(String columnName)
	{
		return null;
	}

	public BigDecimal getBigDecimal(int columnIndex)
	{
		return null;
	}

	public BigDecimal getBigDecimal(int columnIndex, int scale)
	{
		return null;
	}

	public BigDecimal getBigDecimal(String columnName)
	{
		return null;
	}

	public BigDecimal getBigDecimal(String columnName, int scale)
	{
		return null;
	}

	public InputStream getBinaryStream(int columnIndex)
	{
		return null;
	}

	public InputStream getBinaryStream(String columnName)
	{
		return null;
	}

	public Blob getBlob(int i)
	{
		return null;
	}

	public Blob getBlob(String colName)
	{
		return null;
	}

	public boolean getBoolean(int columnIndex) throws SQLException
	{
		return getDouble(columnIndex) != 0;
	}

	public boolean getBoolean(String columnName) throws SQLException
	{
		return getDouble(columnName) != 0;
	}

	public byte getByte(int columnIndex) throws SQLException
	{
		return (byte)getDouble(columnIndex);
	}

	public byte getByte(String columnName) throws SQLException
	{
		return (byte)getDouble(columnName);
	}

	public byte[] getBytes(int columnIndex) throws SQLException
	{
		return getString(columnIndex).getBytes();
	}

	public byte[] getBytes(String columnName) throws SQLException
	{
		return getString(columnName).getBytes();
	}

	public Reader getCharacterStream(int columnIndex)
	{
		return null;
	}

	public Reader getCharacterStream(String columnName)
	{
		return null;
	}

	public Clob getClob(int i)
	{
		return null;
	}

	public Clob getClob(String colName)
	{
		return null;
	}

	public int getConcurrency()
	{
		return RegConnection.TYPE_READ;
	}

	public String getCursorName()
	{
		return null;
	}

	public Date getDate(int columnIndex)
	{
		return null;
	}

	public Date getDate(int columnIndex, Calendar cal)
	{
		return null;
	}

	public Date getDate(String columnName)
	{
		return null;
	}

	public Date getDate(String columnName, Calendar cal)
	{
		return null;
	}

	public double getDouble(int columnIndex) throws SQLException
	{
		return getDouble(getColumn(columnIndex));
	}

	public double getDouble(String columnName) throws SQLException
	{
		try
		{
			Object val = getObject(columnName);
			if (val == null)
				return 0;
			else
				return Double.parseDouble(val.toString());
		}
		catch (Exception e) { throw new SQLException(e.toString()); }
	}

	public int getFetchDirection()
	{
		return FETCH_FORWARD;
	}

	public int getFetchSize()
	{
		return (records == null) ? 0 : records.size();
	}

	public float getFloat(int columnIndex) throws SQLException
	{
		return (float)getDouble(columnIndex);
	}

	public float getFloat(String columnName) throws SQLException
	{
		return (float)getDouble(columnName);
	}

	public int getInt(int columnIndex) throws SQLException
	{
		return (int)getDouble(columnIndex);
	}

	public int getInt(String columnName) throws SQLException
	{
		return (int)getDouble(columnName);
	}

	public int getHoldability()
	{
		return RegConnection.TYPE_HOLD;
	}
	
	public long getLong(int columnIndex) throws SQLException
	{
		return (long)getDouble(columnIndex);
	}

	public long getLong(String columnName) throws SQLException
	{
		return (long)getDouble(columnName);
	}

	public ResultSetMetaData getMetaData()
	{
		return new RegMetaData(fields);
	}

	public Reader getNCharacterStream(int columnIndex)
	{
		return null;
	}

	public Reader getNCharacterStream(String columnLabel)
	{
		return null;
	}

	public NClob getNClob(int columnIndex)
	{
		return null;
	}

	public NClob getNClob(String columnLabel)
	{
		return null;
	}

	public String getNString(int columnIndex)
	{
		return null;
	}

	public String getNString(String columnLabel)
	{
		return null;
	}
	
	public Object getObject(int columnIndex) throws SQLException
	{
		return getObject(getColumn(columnIndex));
	}

	public Object getObject(int i, Map map) throws SQLException
	{
		return getObject(i);
	}

	public Object getObject(String columnName) throws SQLException
	{
		int n = 0;
		if (records == null || current < 0 || current >= (n = records.size()))
			throw new SQLException("No valid row to read from");
		Object val = records.get(current);
		if (!(val instanceof Map))
			throw new SQLException("Invalid class "+val.getClass().getName()+" for row");
		Map rec = (Map)val;
		String fld = columnName.toLowerCase();
		if (columnName == null || columnName.equals("") || !rec.containsKey(fld))
			throw new SQLException("Invalid column name "+fld+" for row #"+(current+1)+":\n  "+rec);
		return rec.get(fld);
	}

	public Object getObject(String colName, Map map) throws SQLException
	{
		return getObject(colName);
	}

	public Ref getRef(int i)
	{
		return null;
	}

	public Ref getRef(String colName)
	{
		return null;
	}

	public int getRow()
	{
		if (records == null || current < 0 || current >= records.size())
			return 0;
		else
			return current+1;
	}

	public RowId getRowId(int columnIndex)
	{
		return null;
	}

	public RowId getRowId(String columnLabel)
	{
		return null;
	}

	public SQLXML getSQLXML(int columnIndex)
	{
		return null;
	}

	public SQLXML getSQLXML(String columnLabel) 
	{
		return null;
	}
	 
	public short getShort(int columnIndex) throws SQLException
	{
		return (short)getDouble(columnIndex);
	}

	public short getShort(String columnName) throws SQLException
	{
		return (short)getDouble(columnName);
	}

	public Statement getStatement()
	{
		return statement;
	}

	public String getString(int columnIndex) throws SQLException
	{
		return getString(getColumn(columnIndex));
	}

	public String getString(String columnName) throws SQLException
	{
		Object val = getObject(columnName);
		return (val == null) ? "" : val.toString();
	}

	public Time getTime(int columnIndex)
	{
		return null;
	}

	public Time getTime(int columnIndex, Calendar cal)
	{
		return null;
	}

	public Time getTime(String columnName)
	{
		return null;
	}

	public Time getTime(String columnName, Calendar cal)
	{
		return null;
	}

	public Timestamp getTimestamp(int columnIndex)
	{
		return null;
	}

	public Timestamp getTimestamp(int columnIndex, Calendar cal)
	{
		return null;
	}

	public Timestamp getTimestamp(String columnName)
	{
		return null;
	}

	public Timestamp getTimestamp(String columnName, Calendar cal)
	{
		return null;
	}

	public int getType()
	{
		return RegConnection.TYPE_SET;
	}

	public InputStream getUnicodeStream(int columnIndex)
	{
		return null;
	}

	public InputStream getUnicodeStream(String columnName)
	{
		return null;
	}

	public URL getURL(int columnIndex)
	{
		return null;
	}

	public URL getURL(String columnName)
	{
		return null;
	}

	public SQLWarning getWarnings()
	{
		return null;
	}

	public void insertRow() throws SQLException
	{
		throw new SQLException("RecordSet is read only");
	}

	public boolean isAfterLast()
	{
		return (records != null && current >= records.size());
	}

	public boolean isBeforeFirst()
	{
		return (records != null && current < 0);
	}

	public boolean isClosed()
	{
		return (records == null || fields == null);
	}
	
	public boolean isFirst()
	{
		return (records != null && current == 0);
	}

	public boolean isLast()
	{
		return (records != null && current+1 == records.size());
	}

	public boolean last()
	{
		if (records == null)
			return false;
		int n = records.size();
		if (n == 0)
			return false;
		current = n - 1;
		return true;
	}

	public void moveToCurrentRow()
	{
	}

	public void moveToInsertRow() throws SQLException
	{
		throw new SQLException("RecordSet is read only");
	}

	public boolean next()
	{
		if (records == null)
			return false;
		int n = records.size();
		if (current >= n)
			return false;
		current++;
		if (current >= n)
			return false;
		return true;
	}

	public boolean previous()
	{
		if (current < 0 || records == null || records.size() == 0)
			return false;
		current--;
		if (current < 0)
			return false;
		return true;
	}

	public void refreshRow()
	{
	}

	public boolean relative(int rows)
	{
		return false;
	}

	public boolean rowDeleted()
	{
		return false;
	}

	public boolean rowInserted()
	{
		return false;
	}

	public boolean rowUpdated()
	{
		return false;
	}

	public void setFetchDirection(int direction)
	{
	}

	public void setFetchSize(int rows)
	{
	}

	public void updateArray(int columnIndex, Array x)
	{
	}

	public void updateArray(String columnName, Array x)
	{
	}

	public void updateAsciiStream(int columnIndex, InputStream x)
	{
	}

	public void updateAsciiStream(int columnIndex, InputStream x, int length)
	{
	}

	public void updateAsciiStream(int columnIndex, InputStream x, long length)
	{
	}

	public void updateAsciiStream(String columnName, InputStream x)
	{
	}

	public void updateAsciiStream(String columnName, InputStream x, int length)
	{
	}

	public void updateAsciiStream(String columnName, InputStream x, long length)
	{
	}

	public void updateBigDecimal(int columnIndex, BigDecimal x)
	{
	}

	public void updateBigDecimal(String columnName, BigDecimal x)
	{
	}

	public void updateBinaryStream(int columnIndex, InputStream x)
	{
	}

	public void updateBinaryStream(int columnIndex, InputStream x, int length)
	{
	}

	public void updateBinaryStream(int columnIndex, InputStream x, long length)
	{
	}

	public void updateBinaryStream(String columnName, InputStream x)
	{
	}

	public void updateBinaryStream(String columnName, InputStream x, int length)
	{
	}

	public void updateBinaryStream(String columnName, InputStream x, long length)
	{
	}

	public void updateBlob(int columnIndex, Blob x)
	{
	}

	public void updateBlob(int columnIndex, InputStream x)
	{
	}

	public void updateBlob(int columnIndex, InputStream x, long length)
	{
	}

	public void updateBlob(String columnName, Blob x)
	{
	}

	public void updateBlob(String columnName, InputStream x)
	{
	}

	public void updateBlob(String columnName, InputStream x, long length)
	{
	}

	public void updateBoolean(int columnIndex, boolean x)
	{
	}

	public void updateBoolean(String columnName, boolean x)
	{
	}

	public void updateByte(int columnIndex, byte x)
	{
	}

	public void updateByte(String columnName, byte x)
	{
	}

	public void updateBytes(int columnIndex, byte[] x)
	{
	}

	public void updateBytes(String columnName, byte[] x)
	{
	}

	public void updateCharacterStream(int columnIndex, Reader x)
	{
	}

	public void updateCharacterStream(int columnIndex, Reader x, int length)
	{
	}

	public void updateCharacterStream(int columnIndex, Reader x, long length)
	{
	}

	public void updateCharacterStream(String columnName, Reader reader)
	{
	}

	public void updateCharacterStream(String columnName, Reader reader, int length)
	{
	}

	public void updateCharacterStream(String columnName, Reader reader, long length)
	{
	}

	public void updateClob(int columnIndex, Clob x)
	{
	}

	public void updateClob(String columnName, Clob x)
	{
	}

	public void updateClob(int columnIndex, Reader reader)
	{
	}

	public void updateClob(int columnIndex, Reader reader, long length)
	{
	}

	public void updateClob(String columnLabel, Reader reader)
	{
	}

	public void updateClob(String columnLabel, Reader reader, long length)
	{
	}

	public void updateDate(int columnIndex, Date x)
	{
	}

	public void updateDate(String columnName, Date x)
	{
	}

	public void updateDouble(int columnIndex, double x)
	{
	}

	public void updateDouble(String columnName, double x)
	{
	}

	public void updateFloat(int columnIndex, float x)
	{
	}

	public void updateFloat(String columnName, float x)
	{
	}

	public void updateInt(int columnIndex, int x)
	{
	}

	public void updateInt(String columnName, int x)
	{
	}

	public void updateLong(int columnIndex, long x)
	{
	}

	public void updateLong(String columnName, long x)
	{
	}

	public void updateNCharacterStream(int columnIndex, Reader x)
	{
	}

	public void updateNCharacterStream(int columnIndex, Reader x, long length)
	{
	}

	public void updateNCharacterStream(String columnLabel, Reader reader)
	{
	}

	public void updateNCharacterStream(String columnLabel, Reader reader, long length)
	{
	}

	public void updateNClob(int columnIndex, NClob nClob)
	{
	}

	public void updateNClob(int columnIndex, Reader reader)
	{
	}

	public void updateNClob(int columnIndex, Reader reader, long length)
	{
	}

	public void updateNClob(String columnLabel, NClob nClob)
	{
	}

	public void updateNClob(String columnLabel, Reader reader)
	{
	}

	public void updateNClob(String columnLabel, Reader reader, long length)
	{
	}

	public void updateNString(int columnIndex, String nString)
	{
	}

	public void updateNString(String columnLabel, String nString) 
	{
	}

	public void updateRowId(int columnIndex, RowId x)
	{
	}

	public void updateRowId(String columnLabel, RowId x)
	{
	}

	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
	{
	}

	public void updateSQLXML(String columnLabel, SQLXML xmlObject)
	{
	}
	
 	public void updateNull(int columnIndex)
	{
	}

	public void updateNull(String columnName)
	{
	}

	public void updateObject(int columnIndex, Object x)
	{
	}

	public void updateObject(int columnIndex, Object x, int scale)
	{
	}

	public void updateObject(String columnName, Object x)
	{
	}

	public void updateObject(String columnName, Object x, int scale)
	{
	}

	public void updateRef(int columnIndex, Ref x)
	{
	}

	public void updateRef(String columnName, Ref x)
	{
	}

	public void updateRow()
	{
	}

	public void updateShort(int columnIndex, short x)
	{
	}

	public void updateShort(String columnName, short x)
	{
	}

	public void updateString(int columnIndex, String x)
	{
	}

	public void updateString(String columnName, String x)
	{
	}

	public void updateTime(int columnIndex, Time x)
	{
	}

	public void updateTime(String columnName, Time x)
	{
	}

	public void updateTimestamp(int columnIndex, Timestamp x)
	{
	}

	public void updateTimestamp(String columnName, Timestamp x)
	{
	}

	public boolean wasNull()
	{
		return (last == null);
	}
	
	public boolean isWrapperFor(Class iface)
	{
		return false;
	}
	
	public Object unwrap(Class iface)
	{
		return null;
	}
}



