package org.regadou.jss;

import java.sql.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import org.mozilla.javascript.*;

public class Database
{
   private Connection con;
   private Statement st;
   private String driver, src, defaultid="id", limits[]={"",""};
   private ResultSet lastrs = null;
	private int min=0, max=0, vendor=-1;
   private int nbrec=-1, atrec=0;
   public static final int VENDORNAME=0, PWINURL=1, DRIVERNAME=2, LIMITSTART=3, LIMITEND=4;
   public static final int DERBY=0, HSQLDB=1, MYSQL=2, ODBC=3, ORACLE=4, POSTGRESQL=5, ACCESS=6, SQLSERVER=7;
   public static final String DRIVERS[][] =
   {
      {"derby",      "p",  "org.apache.derby.jdbc.EmbeddedDriver",         "",   ""},
      {"hsqldb",     null, "org.hsqldb.jdbcDriver",                        "",   ""},
      {"mysql",      "p",  "com.mysql.jdbc.Driver",                        "`",  "`"},
      {"odbc",       "p",  "sun.jdbc.odbc.JdbcOdbcDriver",                 "",   ""},
		{"oracle",		null,	"oracle.jdbc.driver.OracleDriver",              "",   ""},
      {"postgresql", "p",  "org.postgresql.Driver",                        "\"", "\""},
      {"access",     "p",  "org.regadou.jmdb.MDBDriver",                   "[",  "]"},
      {"sqlserver",  "p",  "com.microsoft.sqlserver.jdbc.SQLServerDriver", "\"", "\""}
   };

   public Database(String url) throws ClassNotFoundException,
                                      IllegalAccessException,
                                      InstantiationException,
                                      SQLException
   {
      if (url == null || url.equals(""))
         throw new RuntimeException("Invalid empty url");
      String txt = null;
      for (int p = 0; p >= 0; p++)
      {
         int s = p;
         p = url.indexOf(':', s);
         if (p < 0) break;
         txt = url.substring(s, p);
         for (int d = 0; d < DRIVERS.length; d++)
         {
            if (txt.equals(DRIVERS[d][VENDORNAME]))
            {
               src = url;
               vendor = d;
               driver = DRIVERS[d][DRIVERNAME];
               Class.forName(driver).newInstance();
               if (DRIVERS[d][PWINURL] == null)
               {
                  String user = getcgi(url, "user");
                  if (user == null) user = "";
                  String pass = getcgi(url , "password");
                  if (pass == null) pass = "";
                  int i = url.indexOf('?');
                  if (i > 0) url = url.substring(0,i);
                  con = DriverManager.getConnection(url, user, pass);
               }
               if (con == null) con = DriverManager.getConnection(url);
               st = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
													 ResultSet.CONCUR_READ_ONLY);
					limits[0] = DRIVERS[d][LIMITSTART];
					limits[1] = DRIVERS[d][LIMITEND];
					Interpreter.register(this);
               return;
            }
         }
      }
      throw new ClassNotFoundException("No driver found for "+url);
   }


   private String getcgi(String url, String fld)
   {
      int len = 0;
      if (url == null || fld == null || (len = fld.length()) == 0)
         return null;
      String val = null;
      int p = url.indexOf('?');
      while (p >= 0)
      {
         int s = p+1;
         p = url.indexOf('&',s);
         if (url.startsWith(fld+"=", s))
         {
            val = url.substring(s+len+1, p);
            break;
         }
      }
      return val;
   }

   public String toString()
   {
      return "[Object Database]";
   }

   public String toSource()
   {
      return "(new Database(\""+src+"\"))";
   }

   public String getClassName()
   {
       return "Database";
   }

   public String getURL()
   {
      return src;
   }

	public void close()
	{
		try
		{
			if (lastrs != null)
			{
				lastrs.close();
				lastrs = null;
			}
			if (st != null)
			{
				st.close();
				st = null;
			}
			if (con != null)
			{
				con.close();
				con = null;
			}
		}
      catch (Exception e) { throw new RuntimeException(e.toString()); }
      finally { Interpreter.unregister(this); }
	}
	
	public void create(String table, Scriptable schema) throws SQLException
	{
		if (table == null || table.equals("") || table.indexOf(" ") >= 0)
			throw new RuntimeException("Invalid table name to create: "+table);
		else if (schema == null)
			throw new RuntimeException("No field in table "+table+" schema");
	   query(getTableQuery(table, schema));
	}
	
   public int recordCount() throws SQLException
   {
      return recordCount(lastrs);
   }

   public int recordCount(ResultSet rs) throws SQLException
   {
      if (rs == null || rs.getType() == ResultSet.TYPE_FORWARD_ONLY)
         return -1;
		rs.last();
      int nb = rs.getRow();
      rs.beforeFirst();
      return nb;
   }

   public ResultSet query(String q) throws SQLException
   {
		min = max = 0;
      int len = q.length();
      lastrs = null;
      for (int i = 0; i < len; i++)
      {
         char c = q.charAt(i);
         if (c == 's' || c == 'S')
         {
            lastrs = st.executeQuery(q);
            return lastrs;
         }
         else if ((c == 'd' || c == 'D') && q.substring(i).toLowerCase().startsWith("describe"))
         {
            lastrs = st.executeQuery(q);
            return lastrs;
         }
         else if (c >= 'A')
         {
            st.executeUpdate(q);
            return null;
         }
      }
      throw new SQLException("No query to execute");
   }

   public String getVendor()
   {
      try
      {
			Scriptable obj = (Scriptable)(getMetaData("product")[0]);
			return obj.get("name", obj).toString();
		}
      catch (Exception e) { throw new RuntimeException(e.toString()); }
	}
	
   public Scriptable getDatabases() throws SQLException
   {
		Object recs[] = getMetaData("catalog");
      if (recs == null)
         return null;
      for (int i = 0; i < recs.length; i++)
      {
         Scriptable obj = (Scriptable)recs[i];
         recs[i] = obj.get("table_cat", obj).toString();
      }
      return Interpreter.newObject(recs);
   }

   public Scriptable getTables() throws SQLException
   {
		min = max = 0;
      int p = src.indexOf('?');
      String db = (p >= 0) ? src.substring(0,p) : src;
      p = db.lastIndexOf('/');
      if (p >= 0) db = db.substring(p+1);
      Object recs[] = getMetaData("table", db);
      ArrayList lst = new ArrayList();
      for (int i = 0; i < recs.length; i++)
      {
         Scriptable obj = (Scriptable)recs[i];
         String type = obj.get("table_type", obj).toString().toLowerCase();
         if (type.equals("table"))
            lst.add(obj.get("table_name", obj).toString());
      }
      return Interpreter.newObject(lst.toArray());
   }
   
   public String getTable(String table)
   {
      return getTable(table, true);
   }
   
   public String getTable(String table, boolean getdata)
   {
		try
		{
         Scriptable def = getFields(table);
         String sql = getTableQuery(table, def);
         if (!getdata)
            return sql;
         sql += ";\n";
         ResultSet rs = query("select * from "+table);
         while (rs.next())
            sql += getRecordQuery(table, rs)+";\n";
         return sql;
      }
      catch (Exception e) { return e.toString(); }
   }

   private String getRecordQuery(String table, ResultSet rs)
   {
      try
      {
         Scriptable rec = getRecord(rs);
         String flds = "insert into "+table;
         String vals = ")values";
         Object keys[] = rec.getIds();
         char sep = '(';
         for (int i = 0; i < keys.length; i++)
         {
            String key = keys[i].toString();
            flds += sep+key;
            vals += sep+getValue(rec.get(key, rec));
            sep = ',';         
         }
         return flds+vals+")";
      }
      catch (Exception e) { return e.toString(); }         
   }
   
   public Scriptable getFields(String table) throws SQLException
   {
		min = max = 0;
      int p = src.indexOf('?');
      String db = (p >= 0) ? src.substring(0,p) : src;
      p = db.lastIndexOf('/');
      if (p >= 0) db = db.substring(p+1);
		Object recs[] = getMetaData("column", db+","+table);
      if (recs == null)
         return null;
      Scriptable obj = Interpreter.newObject(null);
      for (int i = 0; i < recs.length; i++)
      {
         Scriptable fld = (Scriptable)recs[i];
			String name = fld.get("column_name", fld).toString().toLowerCase();
			String type = getType(((Number)fld.get("data_type", fld)).intValue());
			Object size = fld.get("column_size", fld);
			ArrayList val = new ArrayList();
			val.add(type);
			val.add(size);
         obj.put(name, obj, Interpreter.newObject(val.toArray()));
      }
      return obj;
   }

   public Scriptable getFields()
   {
      return getFields(lastrs);
   }

   public Scriptable getFields(ResultSet rs)
   {
      if (rs == null)
         return null;
      try
      {
         Scriptable def = Interpreter.newObject(null);
         ResultSetMetaData meta = rs.getMetaData();
         int nf = meta.getColumnCount();
         for (int f = 1; f <= nf; f++)
			{
            String name = meta.getColumnName(f).toLowerCase();
            String type = getType(meta.getColumnType(f));
				Integer size = new Integer(meta.getColumnDisplaySize(f));
				ArrayList val = new ArrayList();
				val.add(type);
				val.add(size);
            def.put(name, def, Interpreter.newObject(val.toArray()));
			}
         return def;
      }
      catch (Exception e) { throw new RuntimeException(e.toString()); }
   }

   public Scriptable getFieldNames(String table)
   {
      try
      {
			min = max = 0;
         int p = src.indexOf('?');
         String db = (p >= 0) ? src.substring(0,p) : src;
         p = db.lastIndexOf('/');
         if (p >= 0) db = db.substring(p+1);
         Object recs[] = getMetaData("column", db+","+table);
         if (recs == null || recs.length == 0)
            return null;
			Object lst[] = new Object[recs.length];
         for (int f = 1; f <= recs.length; f++)
			{
				Scriptable fld = (Scriptable)recs[f];
            lst[f-1] = fld.get("column_name", fld).toString().toLowerCase();
         }
         return Interpreter.newObject(lst);
     }
      catch (Exception e) { throw new RuntimeException(e.toString()); }
   }

   public Scriptable getFieldNames()
   {
      return getFieldNames(lastrs);
   }

   public Scriptable getFieldNames(ResultSet rs)
   {
      if (rs == null)
         return null;
      try
      {
         ResultSetMetaData meta = rs.getMetaData();
         int nf = meta.getColumnCount();
			Object lst[] = new Object[nf];
         for (int f = 1; f <= nf; f++)
            lst[f-1] = meta.getColumnName(f).toLowerCase();
         return Interpreter.newObject(lst);
      }
      catch (Exception e) { throw new RuntimeException(e.toString()); }
   }

   public Scriptable getRecords() throws SQLException
   {
      Object recs[] = getRecordArray(lastrs);
      if (recs == null)
         return null;
      else
         return Interpreter.newObject(recs);
   }

   public Scriptable getRecords(ResultSet rs) throws SQLException
   {
      Object recs[] = getRecordArray(rs);
      if (recs == null)
         return null;
      else
         return Interpreter.newObject(recs);
   }

   public Scriptable getRecords(String q) throws SQLException
   {
      Object recs[] = getRecordArray(query(q));
      if (recs == null)
         return null;
      else
         return Interpreter.newObject(recs);
   }

   public Object[] getRecordArray(ResultSet rs) throws SQLException
   {
      if (rs == null)
      {
         if (lastrs == null)
             return null;
         rs = lastrs;
      }
      Object recs[] = null;
      int nr = recordCount(rs);
      if (nr >= 0)
      {
         recs = new Object[nr];
         for (int r = 0; rs.next(); r++)
            recs[r] = getRecord(rs);
      }
      else
      {
         ArrayList lst = new ArrayList();
         while (rs.next())
            lst.add(getRecord(rs));
			recs = lst.toArray();
      }
      return recs;
   }

   public Scriptable getRecord() throws SQLException
   {
      return getRecord(lastrs);
   }

   public Scriptable getRecord(ResultSet rs) throws SQLException
   {
      if (rs == null)
      {
         if (lastrs == null)
            return null;
         rs = lastrs;
      }
      ResultSetMetaData meta = rs.getMetaData();
      int nf = meta.getColumnCount();
      Scriptable rec = Interpreter.newObject(null);
      for (int f = 1; f <= nf; f++)
      {
         String fld = meta.getColumnName(f).toLowerCase();
         Object val = rs.getObject(f);
			if (val == null)
				;
         else if (val instanceof java.util.Date)
            val = Interpreter.newObject(val);
         else if (val instanceof java.sql.Blob)
			{
				java.sql.Blob bytes = (java.sql.Blob)val;
				val = new String(bytes.getBytes((long)1, (int)bytes.length()));
			}
         else if (val instanceof java.sql.Clob)
			{
				java.sql.Clob txt = (java.sql.Clob)val;
				val = txt.getSubString((long)1, (int)txt.length());
			}
			else if (val.getClass().isArray()
					&& val.getClass().getComponentType() == java.lang.Byte.TYPE)
				val = (new String((byte[])val)).trim();
         else if (!(val instanceof Number)
               && !(val instanceof Boolean))
            val = val.toString().trim();
         rec.put(fld, rec, val);
      }
		return rec;
   }

   public Scriptable getRecord(String table) throws SQLException
   {
      return getRecord(table, lastrs);
   }

   public Scriptable getRecord(String table, ResultSet rs) throws SQLException
   {
      if (rs == null)
      {
         if (lastrs == null)
            return null;
         rs = lastrs;
      }
      ResultSetMetaData meta = rs.getMetaData();
      int nf = meta.getColumnCount();
      Scriptable rec = Interpreter.newObject(null);
      for (int f = 1; f <= nf; f++)
      {
         String fld = meta.getColumnName(f).toLowerCase();
         Object val = getDefaultValue(meta.getColumnType(f));
         rec.put(fld, rec, val);
      }
		return rec;
   }

   public Map getRecordMap(ResultSet rs) throws SQLException
   {
      if (rs == null)
      {
         if (lastrs == null)
            return null;
         rs = lastrs;
      }
      ResultSetMetaData meta = rs.getMetaData();
      int nf = meta.getColumnCount();
      Map rec = new HashMap();
      for (int f = 1; f <= nf; f++)
      {
         String fld = meta.getColumnName(f).toLowerCase();
         Object val = rs.getObject(f);
			if (val == null)
				;
         else if (val instanceof java.sql.Blob)
			{
				java.sql.Blob bytes = (java.sql.Blob)val;
				val = new String(bytes.getBytes((long)1, (int)bytes.length()));
			}
         else if (val instanceof java.sql.Clob)
			{
				java.sql.Clob txt = (java.sql.Clob)val;
				val = txt.getSubString((long)1, (int)txt.length());
			}
			else if (val.getClass().isArray()
					&& val.getClass().getComponentType() == java.lang.Byte.TYPE)
				val = (new String((byte[])val)).trim();
         else if (!(val instanceof Number)
               && !(val instanceof Boolean)
               && !(val instanceof java.util.Date))
            val = val.toString().trim();
         rec.put(fld, val);
      }
		return rec;
   }

   public void setRecord(Scriptable rec, String table)
   {
      setRecord(rec, table, defaultid);
   }

   public void setRecord(Scriptable rec, String table, String idfld)
   {
      if (rec == null || table == null || idfld == null)
         throw new RuntimeException("One of record or table or idfld is invalid");
      boolean update=false, gotid=false, others=false;
      String q="", more="", end="";
      Object keys[] = rec.getIds();
      for (int i = 0; i < keys.length; i++)
      {
         String fld = keys[i].toString();
         if (idfld.equals(fld))
         {
				gotid = true;
            Object val = rec.get(fld, rec);
            if (val != null)
            {
               update = true;
               q = "update "+limits[0]+table+limits[1]+" set ";
               end = " where "+idfld+"="+Context.toString(val);
            }
            break;
         }
			else
				others = true;
      }

      if (!update)
      {
         int id = nextid(table, idfld);
         String idval = String.valueOf(id);
         rec.put(idfld, rec, idval);
         q = "insert into "+limits[0]+table+limits[1]+"(";
         more = ") values(";
         end = ")";
			if (gotid)
			{
				String virgule = others ? "," : "";
				q += idfld+virgule;
				more += id+virgule;
			}
      }

      String sep = "";
      for (int i = 0; i < keys.length; i++)
      {
         String fld = keys[i].toString();
         if (fld.equals(idfld))
            continue;
         String txt = getValue(rec.get(fld, rec));
         q += sep+fld;
         if (update)
            q += "="+txt;
         else
            more += sep+txt;
         sep = ",";
      }

		q += more+end;
      try
      {
         query(q);
      }
      catch (Exception e)
      {
         throw new RuntimeException("SQL error with the following query:<br>\n"
			                           +q+"<br>\n"+e.toString());
      }
   }

   public int nextid(String tab, String idfld)
   {
      int n = 0;
      try
      {
         ResultSet rs = query("select max("+idfld+") from "+tab);
         if (rs != null)
         {
            try { if (rs.next()) n = Integer.parseInt(rs.getString(1)); }
            catch (Exception e) {}
         }
      }
      catch (Exception e) {}
      return n+1;
   }

   private String getValue(Object val)
   {
		if (val instanceof Scriptable)
		{
			Scriptable obj = (Scriptable)val;
			String type = obj.getClassName().toLowerCase();
			if (type.indexOf("date") >= 0)
			{
				val = ScriptableObject.callMethod(obj, "getTime", new Object[]{});
				if (val instanceof Number)
					val = new java.util.Date(((Number)val).longValue());
				else
					throw new RuntimeException("Cannot get valid time from Javascript Date object");
			}
		}

      String txt = null;
		if (val == null)
			txt = "null";
      else if (val instanceof Number)
         txt = val.toString();
      else if (val instanceof Boolean)
		{
			if (vendor == DERBY || vendor == ORACLE)
				txt = ((Boolean)val).booleanValue() ? "1" : "0";
			else
				txt = val.toString();
		}
		else if (val instanceof java.util.Date)
		{
			java.util.Date dt = (java.util.Date)val;
			txt = "{ts '"+(dt.getYear()+1900)+"-"
				 + String.valueOf(dt.getMonth()+101).substring(1)+"-"
				 + String.valueOf(dt.getDate()+100).substring(1)+" "
				 + String.valueOf(dt.getHours()+100).substring(1)+":"
				 + String.valueOf(dt.getMinutes()+100).substring(1)+":"
				 + String.valueOf(dt.getSeconds()+100).substring(1)+"'}";
		}
      else
         txt = sqlstring(Context.toString(val));
      return txt;
   }
   
   public static String sqlstring(String src)
   {
      if (src == null)
         return "null";
      else if (src.equals(""))
         return "''";
      String dst = "'";
      int len = src.length();
      for (int i = 0; i < len; i++)
      {
         char c = src.charAt(i);
         switch (c)
         {
         case '\'':
         case '\\':
            dst += c;
            break;
         }
         dst += c;
      }
      return dst+"'";
   }
	
	public void setLimit(int lo, int hi)
	{
		if (lo <= 0 || hi <= 0)
			min = max = 0;
		else if (hi < lo)
		{
			min = hi;
			max = lo;
		}
		else
		{
			min = lo;
			max = hi;
		}
	}
	
	private String getTableQuery(String table, Scriptable schema)
	{
		Object flds[] = schema.getIds();
		if (flds.length == 0)
			throw new RuntimeException("No field in table "+table+" schema");
		String sql = "create table "+table;
		for (int f = 0; f < flds.length; f++)
		{
			String name = flds[f].toString();
			sql += ((f == 0) ? "\n(\n   " : ",\n   ")+name+" ";
			Object val = schema.get(name, schema);
			if (val == null || !(val instanceof Scriptable))
				throw new RuntimeException("Bad definition for field "+name+" in table "+table+" schema");
			Scriptable def = (Scriptable)val;			
			val = def.get(0, def);
			String type = null;
			int len = 0;
			if (val == null)
				type = def.toString().toLowerCase();
			else
			{				
				type = val.toString().toLowerCase();
				val = def.get(1, def);
				if (val != null && (val instanceof Number))
					len = ((Number)val).intValue();
			}
			if (type.equals("boolean"))
			{
				if (vendor == DERBY || vendor == ACCESS)
					sql += "smallint";
				else
					sql += "boolean";
			}
			else if (type.equals("number"))
				sql += "real";
			else if (type.equals("integer"))
				sql += "integer";
			else if (type.equals("date"))
			{
				switch (vendor)
				{
				case POSTGRESQL:
				case DERBY:
				case HSQLDB:
					sql += "timestamp"; break;				
				case MYSQL:
				case ACCESS:
				case SQLSERVER:
					sql += "datetime"; break;			
				case ORACLE:
					sql += "date"; break;				
				default:
					throw new RuntimeException("Don't know the date type for "+getVendor());
				}
			}
			else if (type.equals("binary"))
			{
				switch (vendor)
				{
				case POSTGRESQL:
					sql += "bytea"; break;				
				case MYSQL:
					sql += "longblob"; break;				
				case DERBY:
					sql += "blob"; break;				
				case HSQLDB:
					sql += "binary"; break;				
				case ACCESS:
				case SQLSERVER:
					sql += "longbinary"; break;			
				case ORACLE:
					sql += "long raw"; break;				
				default:
					throw new RuntimeException("Don't know the binary type for "+getVendor());
				}
			}
			else if (type.equals("string"))
			{
				if (len > 0 && len < 256)
					sql += "varchar("+len+")";
				else
				{
					switch (vendor)
					{
					case POSTGRESQL:
					case MYSQL:
						sql += "text"; break;				
					case DERBY:
						sql += "clob"; break;				
					case HSQLDB:
						sql += "longvarchar"; break;				
					case ACCESS:
					case SQLSERVER:
						sql += "longchar"; break;			
					case ORACLE:
						sql += "long"; break;				
					default:
						throw new RuntimeException("Don't know the long string type for "+getVendor());
					}
				}
			}
		}
		
		return sql+"\n)";
	}

	private Object getDefaultValue(int type)
	{
		Object val = null;
		switch (type)
		{
		case Types.BIT:
		case Types.BOOLEAN:
			val = new Boolean(false);
			break;
		case Types.DECIMAL:
		case Types.DOUBLE:
		case Types.FLOAT:
		case Types.NUMERIC:
		case Types.REAL:
			val = new Double(0);
			break;
		case Types.BIGINT:
		case Types.INTEGER:
		case Types.SMALLINT:
		case Types.TINYINT:
			val = new Integer(0);
			break;
		case Types.DATE:
		case Types.TIME:
		case Types.TIMESTAMP:
			val = null;
			break;
		case Types.BLOB:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			val = "";
			break;
		default:
			val = "";
		}
		return val;
	}
	
	private String getType(int src)
	{
		String type = null;
		switch (src)
		{
		case Types.BIT:
		case Types.BOOLEAN:
			type = "Boolean";
			break;
		case Types.DECIMAL:
		case Types.DOUBLE:
		case Types.FLOAT:
		case Types.NUMERIC:
		case Types.REAL:
			type = "Number";
			break;
		case Types.BIGINT:
		case Types.INTEGER:
		case Types.SMALLINT:
		case Types.TINYINT:
			type = "Integer";
			break;
		case Types.DATE:
		case Types.TIME:
		case Types.TIMESTAMP:
			type = "Date";
			break;
		case Types.BLOB:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			type = "Binary";
			break;
		default:
			type = "String";
		}
		return type;
	}

	public Object[] getMetaData(String type) throws SQLException
	{
		return getMetaData(type, null);
	}

	private static final String METADATA[] = {"product","catalog","table","column"};
	private static final int META_PRODUCT=0, META_CATALOG=1, META_TABLE=2, META_COLUMN=3;
	private Object[] getMetaData(String type, String param) throws SQLException
	{
		int t = 0;
		for (; t < METADATA.length; t++)
 			if (METADATA[t].equals(type))
				break;
		if (t >= METADATA.length)
			return new Object[0];
		DatabaseMetaData dmd = con.getMetaData();
		if (dmd == null)
		{
			String q;
			switch (t)
			{
			case META_PRODUCT:
				q = "system product";
				break;
			case META_CATALOG:
				q = "system catalog";
				break;
			case META_TABLE:
				q = "system table";
				break;
			case META_COLUMN:
				String params[] = param.split(",");
				q = "select * from "+params[1];
				break;
			default:
				return new Object[0];
			}
			st.execute(q);
			return getRecordArray(st.getResultSet());
		}
		else
		{
			switch (t)
			{
			case META_PRODUCT:
				String name = dmd.getDatabaseProductName();
				Scriptable rec = Interpreter.newObject(null);
            rec.put("name", rec, name);
				return new Object[]{rec};
			case META_CATALOG:
				return getRecordArray(dmd.getCatalogs());
			case META_TABLE:
				return getRecordArray(dmd.getTables(param,null,null,null));
			case META_COLUMN:
				String params[] = param.split(",");
		   	String db = params[0];
		   	String tab = params[1];
				return getRecordArray(dmd.getColumns(db,null,tab,null));
			}
		}

		return new Object[0];
	}
}



