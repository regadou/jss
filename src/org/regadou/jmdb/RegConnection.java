package org.regadou.jmdb;

import java.util.Properties;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Executor;
import java.sql.*;
import com.healthmarketscience.jackcess.*;

public class RegConnection implements Connection
{
	private Database db = null;
	private ArrayList statements = null;
	public static final int TYPE_HOLD = ResultSet.HOLD_CURSORS_OVER_COMMIT,
									TYPE_READ = ResultSet.CONCUR_READ_ONLY,
									TYPE_SET = ResultSet.TYPE_SCROLL_INSENSITIVE;

	protected RegConnection(Database d)
	{
		db = d;
		if (d != null)
			statements = new ArrayList();
	}

	public void clearWarnings()
	{
	}

	public void close() throws SQLException
	{
		try
		{
			if (statements != null)
			{
				int n = statements.size();
				for (int i = 0; i < n; i++)
					((Statement)(statements.get(i))).close();
				statements = null;
			}
			if (db != null)
			{
				db.close();
				db = null;
			}
		}
		catch (Exception e) { throw new SQLException(e.toString(), e); }
	}

	public void commit()
	{
	}

	public Array createArrayOf(String typeName, Object[] elements)
	{
		return null;
	}

	public Blob createBlob()
	{
		return null;
	}

	public Clob createClob()
	{
		return null;
	}

	public NClob createNClob()
	{
		return null;
	}

	public SQLXML createSQLXML() 
	{
		return null;
	}

	public Statement createStatement() throws SQLException
	{
		return createStatement(TYPE_SET, TYPE_READ, TYPE_HOLD);
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
	{
		return createStatement(TYPE_SET, TYPE_READ, TYPE_HOLD);
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
	{
		if (resultSetType != TYPE_SET || resultSetConcurrency != TYPE_READ || resultSetHoldability != TYPE_HOLD)
			throw new SQLException("This type of cursor is not supported");
		else if (statements == null || db == null)
			throw new SQLException("Database is in a closed state");
		Statement st = new RegStatement(this, db);
		if (st != null)
			statements.add(st);
		return st;
	}

	public Struct createStruct(String typename, Object attributes[])
	{
		return null;
	}
	
	public boolean getAutoCommit()
	{
		return false;
	}

	public String getCatalog()
	{
		return null;
	}

	public Properties getClientInfo()
	{
		return null;
	}
	
	public String getClientInfo(String name)
	{
		return null;
	}
	
	public int getHoldability()
	{
		return TYPE_HOLD;
	}

	public DatabaseMetaData getMetaData()
	{
		return null;
	}

	public int getTransactionIsolation()
	{
		return 0;
	}

	public Map getTypeMap()
	{
		return null;
	}

	public SQLWarning getWarnings()
	{
		return null;
	}

	public boolean isClosed()
	{
		return db == null;
	}

	public boolean isReadOnly()
	{
		return true;
	}

	public boolean isValid(int timeout) throws SQLException
	{
		if (timeout < 0)
			throw new SQLException("Invalid timeout value: "+timeout);
		try { return (db.getTableNames() != null); }
		catch (Exception e) { return false; }
	}
	
	public String nativeSQL(String sql)
	{
		return sql;
	}

	public CallableStatement prepareCall(String sql)
	{
		return null;
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
	{
		return null;
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
	{
		return null;
	}

	public PreparedStatement prepareStatement(String sql)
	{
		return null;
	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
	{
		return null;
	}

	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
	{
		return null;
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
	{
		return null;
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
	{
		return null;
	}

	public PreparedStatement prepareStatement(String sql, String[] columnNames)
	{
		return null;
	}

	public void releaseSavepoint(Savepoint savepoint)
	{
	}

	public void rollback()
	{
	}

	public void rollback(Savepoint savepoint)
	{
	}

	public void setAutoCommit(boolean autoCommit)
	{
	}

	public void setCatalog(String catalog)
	{
	}

	public void setClientInfo(Properties properties)
	{
	}
	
	public void setClientInfo(String name, String value)
	{
	}
	
	public void setHoldability(int holdability)
	{
	}

	public void setReadOnly(boolean readOnly)
	{
	}

	public Savepoint setSavepoint()
	{
		return null;
	}

	public Savepoint setSavepoint(String name)
	{
		return null;
	}

	public void setTransactionIsolation(int level)
	{
	}

	public void setTypeMap(Map map) 
	{
	}
	
	public boolean isWrapperFor(Class iface)
	{
		return false;
	}
	
	public Object unwrap(Class iface)
	{
		return null;
	}
	
	public int getNetworkTimeout() throws SQLException {
	   if (db == null)
	      throw new SQLException("Database connetion is closed");
	   return 0;
	}
	
	public void setNetworkTimeout(Executor executor, int milliseconds) {}
	
	public void abort(Executor executor) throws SQLException {
	   close();
	}
	
	public String getSchema() {
	   return null;
	}
          
   public void setSchema(String schema) {}

}


