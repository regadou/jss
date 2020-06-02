package org.regadou.jmdb;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Date;
import java.sql.*;
import com.healthmarketscience.jackcess.*;

public class RegStatement implements Statement
{
	private String operators = "+-=*/%<>&|!(){}";
	private Database db = null;
	private Connection con = null;
	private ResultSet last = null;
	private ArrayList sets = null;
	private static final String SYNTAX = "SQL syntax error: ";

	protected RegStatement(Connection c, Database d)
	{
		con = c;
		db = d;
		if (d != null)
			sets = new ArrayList();
	}

	private class parseStatus
	{
		public int step=0, esc=0;
		public String cmd=null, tab=null;
		public ArrayList flds=null, cond=null, order=null;
		public boolean isSystem=false;
		public parseStatus()
		{
			flds = new ArrayList();
			cond = new ArrayList();
		}
		public void add(String txt) throws SQLException
		{
			boolean isliteral = false;
			if (txt.charAt(0) == '\'')
				isliteral = true;
			else
				txt = txt.toLowerCase();
			if (isSystem)
			{
				if (flds == null)
					flds = new ArrayList();
				flds.add(txt);
				return;
			}
			
			switch (step)
			{
			case 0:
				if (txt.equals("system"))
					isSystem = true;
				else if (!txt.equals("select"))
					throw new SQLException(SYNTAX+txt+" command unknown or unsupported");
				cmd = txt;
				step++;
				return;
			case 1:
				if (txt.equals("from"))
					step++;
				else if (flds == null)
					throw new SQLException(SYNTAX+" fields already mentionned before "+txt);
				else if (txt.equals("*"))
					flds = null;
				else
					flds.add(txt);
				return;
			case 2:
				if (isliteral)
					throw new SQLException(SYNTAX+" table name cannot be a constant: "+txt);
				tab = txt;
				step++;
				return;
			case 3:
				if (txt.equals("order"))
					step += 2;
				else if (!txt.equals("where"))
					throw new SQLException(SYNTAX+" missing where clause before "+txt);
				else
					step++;
				return;
			case 4:
				if (txt.equals("order"))
					step++;
				else if (txt.equals("and"))
					cond.add("&&");
				else if (txt.equals("or"))
					cond.add("||");
				else if (txt.equals("not"))
					cond.add("!");
				else if (txt.equals("is") || txt.equals("="))
					cond.add("==");
				else if (txt.equals("<>"))
					cond.add("!=");
				else if (txt.equals("{"))
					esc++;
				else if (txt.equals("}"))
					cond.add(")");
				else if (txt.charAt(0) == '\'')
					cond.add(txt);
				else
				{
					cond.add(txt);
					if (esc > 0)
					{
						cond.add("(");
						esc--;
					}
				}
				return;
			case 5:
				if (!txt.equals("by"))
					throw new SQLException(SYNTAX+" missing by token before "+txt);
				step++;
				order = new ArrayList();
				return;
			case 6:
				int n;
				if (isliteral)
					throw new SQLException(SYNTAX+" cannot sort with a constant: "+txt);
				else if (txt.equals("asc"))
					;
				else if (txt.equals("desc") && (n = order.size()) > 0)
					order.set(n-1, "-"+order.get(n-1));
				else
					order.add(txt);
				return;
			default:
				throw new SQLException(SYNTAX+" unknown token "+txt);
			}
		}
	}

	private parseStatus parse(String sql) throws SQLException
	{
		int n = sql.length();
		parseStatus dst = new parseStatus();
		int word = -1;
		boolean isop = false;
		char instr = 0;
		for (int i = 0; i < n; i++)
		{
			char c = sql.charAt(i);
			if (instr != 0)
			{
				if (c == instr)
				{
					if (c == '\'' && sql.charAt(i+1) == '\'')
						;
					else
					{
						dst.add(sql.substring(word, i+((c == '\'')?1:0)));
						word = -1;
						instr = 0;
					}
				}
			}
			else if (c == '\"' || c == '\'' || c == '`')
			{
				word = i + ((c == '\'')?0:1);
				instr = c;
			}
			else if (c <= ' ' || c == ',')
			{
				if (word >= 0)
				{
					dst.add(sql.substring(word, i));
					word = -1;
					isop = false;
				}
			}
			else if ( (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
					 || (c >= '0' && c <= '9') || c == '_')
			{
				if (word < 0)
					word = i;
				else if (isop)
				{
					dst.add(sql.substring(word, i));
					word = i;
					isop = false;
				}
			}
			else if (operators.indexOf(c) >= 0)
			{
				if (word >= 0 && !isop)
					dst.add(sql.substring(word, i));
				if (!isop)
				{
					word = i;
					isop = true;
				}
			}
		}

		if (word >= 0)
			dst.add(sql.substring(word));
		if (dst.isSystem)
			return dst;
		else if (dst.cmd == null)		
			throw new SQLException(SYNTAX+" no command was given: "+sql);		
		else if (dst.tab == null)
			throw new SQLException(SYNTAX+" no table name was given: "+sql);
		else if (dst.flds != null && dst.flds.size() == 0)
			throw new SQLException(SYNTAX+" no column name was given: "+sql);
		else	
			return dst;
	}
	
	private void newresultset(List recs, List flds) throws SQLException
	{
		if (sets == null)
			throw new SQLException("Statement is not opened");
		last = new RegResultSet(this, recs, flds);
		sets.add(last);
	}
	
	protected void removeresultset(ResultSet r)
	{
		if (r == last)
			last = null;
		if (sets != null)
			sets.remove(r);
	}
	
	private void addfield(List lst, Column c) throws SQLException
	{
		int type = Types.VARCHAR;
		try { type = c.getSQLType(); }
		catch (Exception e) {}
		addfield(lst, c.getName(), type, c.getLength(), c.getPrecision(), c.getScale());
	}
	
	private void addfield(List lst, String name, int type, int size) throws SQLException
	{
		int scale = 0;
		switch (type)
		{
		case Types.DECIMAL:
		case Types.DOUBLE:
		case Types.FLOAT:
		case Types.NUMERIC:
		case Types.REAL:
			scale = size - 1;
		}
		addfield(lst, name, type, size, size, scale);
	}
	
	private void addfield(List lst, String name, int type, int size, int precision, int scale) throws SQLException
	{
		Map data = new HashMap();
		data.put("column_realname", name);
		data.put("column_name", name.toLowerCase());
		data.put("column_size", new Integer(size));
		data.put("column_precision", new Integer(precision));
		data.put("column_scale", new Integer(scale));
		data.put("data_type", new Integer(type));
		lst.add(data);
	}
	
	private void addrecord(List lst, List flds, Object vals[])
	{
		Map data = new HashMap();
		int n = flds.size();
		for (int i = 0; i < n; i++)
		{
			Map fld = (Map)(flds.get(i));
			Object val = (i < vals.length) ? vals[i] : null;
			data.put(fld.get("column_name").toString(), val);
		}
		lst.add(data);
	}
	
	private boolean validcondition(List cond, Map rec)
	{
		if (cond == null || cond.size() == 0)
			return true;
// for now no condition will work
		return false;
	}
	
	private void systemquery(parseStatus st) throws SQLException
	{
		if (db == null || sets == null)
			throw new SQLException("Statement is in a closed state");
		else if (st.flds == null || st.flds.size() == 0)
			throw new SQLException("Missing system data to query");
		List flds = new ArrayList();
		List recs = new ArrayList();
		String cmd = st.flds.get(0).toString().toLowerCase();
		if (cmd.equals("product"))
		{
			addfield(flds, "name", Types.VARCHAR, 50);
			addrecord(recs, flds, new Object[]{"Microsoft Access"});
		}
		else if (cmd.equals("catalog"))
		{
			addfield(flds, "table_cat", Types.VARCHAR, 50);
			try { addrecord(recs, flds, new Object[]{db.toString()}); }
			catch (Exception e) {}
		}
		else if (cmd.equals("table"))
		{
			addfield(flds, "table_type", Types.VARCHAR, 10);
			addfield(flds, "table_name", Types.VARCHAR, 50);
			Object lst[] = db.getTableNames().toArray();
			for (int i = 0; i < lst.length; i++)
				addrecord(recs, flds, new Object[]{"table",lst[i].toString()});
		}
		else
			throw new SQLException("Invalid system command: "+cmd);
			
		newresultset(recs, flds);
	}
	
	private void query(String sql, boolean metaonly) throws SQLException
	{
		last = null;
		parseStatus st = parse(sql);
		if (st.isSystem)
		{
			systemquery(st);
			return;
		}
		String target = st.tab.toLowerCase();
		Table table = null;
		Object tablst[] = db.getTableNames().toArray();
		for (int i = 0; i < tablst.length; i++)
		{
			String name = tablst[i].toString();
			if (name.toLowerCase().equals(target))
			{
				try
				{
					table = db.getTable(name);
					if (table != null)
						break;
				}
				catch (IOException e) { throw new SQLException(e.toString()); }
			}
		}
		if (table == null)
			throw new SQLException("Table "+st.tab+" not found");

		List flds = new ArrayList();
		List src = table.getColumns();
		int n = src.size();
		int s = (st.flds == null) ? 0 : st.flds.size();
		if (s == 0)
		{
			for (int i = 0; i < n; i++)
				addfield(flds, (Column)(src.get(i)));
		}
		else
		{
			for (int i = 0; i < s; i++)
			{
				String f = st.flds.get(i).toString().toLowerCase();
				boolean found = false;
				for (int j = 0; j < n; j++)
				{
					Column col = (Column)(src.get(j));
					if (col.getName().toLowerCase().equals(f))
					{
						addfield(flds, col);
						found = true;
						break;
					}
				}
				if (!found)
					throw new SQLException("Field "+f+" not found in "+target);
			}
		}
			
		if (metaonly)
		{
			newresultset(null, flds);
			return;
		}
		
		List recs = new ArrayList();
		n = table.getRowCount();
		String flst[] = new String[flds.size()];
		for (int f = 0; f < flst.length; f++)
			flst[f] = ((Map)(flds.get(f))).get("column_realname").toString();
		for (int i = 0; i < n; i++)
		{
			Map rec = null;
			try { rec = table.getNextRow(); }
			catch (IOException e) { throw new SQLException(e.toString()); }
			if (!validcondition(st.cond, rec))
				continue;
			Object vals[] = new Object[flst.length];
			for (int f = 0; f < flst.length; f++)
				vals[f] = rec.get(flst[f]);
			addrecord(recs, flds, vals);
		}
		
		if (st.order != null && recs.size() > 1)
		{
			Object vals[] = recs.toArray();
			for (int i = st.order.size() - 1; i >= 0; i--)
			{
				String sortfld = st.order.get(i).toString();
				int sortdir = 1;
				if (sortfld.charAt(0) == '-')
				{
					sortdir = -1;
					sortfld = sortfld.substring(1);
				}
				
				OrderBy o = new OrderBy(sortfld, sortdir);
				Arrays.sort(vals, o);
			}
			
			recs = new ArrayList();
			for (int i = 0; i < vals.length; i++)
				recs.add(vals[i]);
		}

		newresultset(recs, flds);
	}
	
	public void addBatch(String sql)
	{
	}

	public void cancel()
	{
	}

	public void clearBatch()
	{
	}

	public void clearWarnings()
	{
	}

	public void close() throws SQLException
	{
		if (sets == null)
			return;
		int n = sets.size();
		for (int i = 0; i < n; i++)
			((ResultSet)(sets.get(i))).close();
		sets = null;
		last = null;
		con = null;
	}

	public boolean execute(String sql) throws SQLException
	{
		query(sql, true);
		return (last != null);
	}

	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
	{
		throw new SQLException("generated keys is not supported");
	}

	public boolean execute(String sql, int[] columnIndexes) throws SQLException
	{
		throw new SQLException("generated keys is not supported");
	}

	public boolean execute(String sql, String[] columnNames) throws SQLException
	{
		throw new SQLException("generated keys is not supported");
	}

	public int[] executeBatch() throws SQLException
	{
		throw new SQLException("batch execution not supported");
	}

	public ResultSet executeQuery(String sql) throws SQLException
	{
		query(sql, false);
		return last;
	}

	public int executeUpdate(String sql) throws SQLException
	{
		throw new SQLException("database is read-only");
	}

	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
	{
		throw new SQLException("database is read-only");
	}

	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
	{
		throw new SQLException("database is read-only");
	}

	public int executeUpdate(String sql, String[] columnNames) throws SQLException
	{
		throw new SQLException("database is read-only");
	}

	public Connection getConnection()
	{
		return con;
	}

	public int getFetchDirection()
	{
		return ResultSet.FETCH_FORWARD;
	}

	public int getFetchSize() throws SQLException
	{
		return (last == null) ? 0 : last.getFetchSize();
	}

	public ResultSet getGeneratedKeys()
	{
		return null;
	}

	public int getMaxFieldSize()
	{
		return 0;
	}

	public int getMaxRows()
	{
		return 0;
	}

	public boolean getMoreResults()
	{
		return false;
	}

	public boolean getMoreResults(int current)
	{
		return false;
	}

	public int getQueryTimeout()
	{
		return 0;
	}

	public ResultSet getResultSet()
	{
		return last;
	}

	public int getResultSetConcurrency()
	{
		return RegConnection.TYPE_READ;
	}

	public int getResultSetHoldability()
	{
		return RegConnection.TYPE_HOLD;
	}

	public int getResultSetType()
	{
		return RegConnection.TYPE_SET;
	}

	public int getUpdateCount() throws SQLException
	{
		throw new SQLException("database is read-only");
	}

	public SQLWarning getWarnings()
	{
		return null;
	}

	public boolean isClosed()
	{
		return (sets == null);
	}
	
	public boolean isCloseOnCompletion() {
	   return false;
	}
	
	public void closeOnCompletion() {}
	public boolean isPoolable()
	{
		return false;
	}
	
	public void setPoolable(boolean poolable)
	{
	}
 
 	public void setCursorName(String name)
	{
	}

	public void setEscapeProcessing(boolean enable)
	{
	}

	public void setFetchDirection(int direction)
	{
	}

	public void setFetchSize(int rows)
	{
	}

	public void setMaxFieldSize(int max)
	{
	}

	public void setMaxRows(int max)
	{
	}

	public void setQueryTimeout(int seconds) 
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
}


class OrderBy implements java.util.Comparator
{
	private static java.text.Collator comparator = java.text.Collator.getInstance(new java.util.Locale("fr"));
	private String fld;
	private int dir;
	
	public OrderBy(String f, int d)
	{
		if (f == null || f.equals("") || d == 0)
			throw new RuntimeException("Cannot sort with empty field or null direction");
		fld = f;
		if (d < 0)
			dir = -1;
		else
			dir = 1;
	}
	
	public int compare(Object o1, Object o2)
	{
		if (!(o1 instanceof Map) || !(o2 instanceof Map))
			throw new RuntimeException("Comparable objects are not implementing the Map interface");
		Map r1 = (Map)o1;
		Map r2 = (Map)o2;
		Object f1 = r1.get(fld);
		Object f2 = r2.get(fld);
		int res = 0;
		if (f1 == null)
		{
			if (f2 != null)
				res = -1;
		}
		else if (f2 == null)
			res = 1;
		else if (!f1.equals(f2))
		{
			if ((f1 instanceof Number) && (f2 instanceof Number))
				res = (((Number)f1).doubleValue() > ((Number)f2).doubleValue()) ? 1 : -1;
			else if ((f1 instanceof Boolean) && (f2 instanceof Boolean))
				res = ((Boolean)f1).booleanValue() ? 1 : -1;
			else if ((f1 instanceof Date) && (f2 instanceof Date))
				res = (((Date)f1).getTime() > ((Date)f2).getTime()) ? 1 : -1;
			else
				res = comparator.compare(f1.toString(), f2.toString());
		}
		return res * dir;
	}
}



