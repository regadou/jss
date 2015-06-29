package org.regadou.jmdb;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.sql.*;

public class RegMetaData implements ResultSetMetaData
{
	private List fields = null;
	private String table = null, catalog = null;
	
	protected RegMetaData(List f)
	{
		this(f, null, null);
	}
	
	protected RegMetaData(List f, String t)
	{
		this(f, t, null);
	}
	
	protected RegMetaData(List f, String t, String c)
	{
		fields = f;
		table = t;
		catalog = c;
		if (f == null)
			fields = new ArrayList();
	}
	
	public Object getData(int column, String name)
	{
		if (fields == null || column < 1 || column > fields.size())
			return null;
		Object val = fields.get(column-1);
		if (!(val instanceof Map))
			return null;
		Map obj = (Map)val;
		return obj.get(name);
	}
	
	public String getCatalogName(int column)
	{
		return catalog;
	}

	public String getColumnClassName(int column)
	{
		String type;
		switch (getColumnType(column))
		{
		case Types.BIT:
		case Types.BOOLEAN:
			type = "java.lang.Boolean";
			break;
		case Types.DECIMAL:
		case Types.DOUBLE:
		case Types.FLOAT:
		case Types.NUMERIC:
		case Types.REAL:
			type = "java.lang.Number";
			break;
		case Types.BIGINT:
		case Types.INTEGER:
		case Types.SMALLINT:
		case Types.TINYINT:
			type = "java.lang.Integer";
			break;
		case Types.DATE:
		case Types.TIME:
		case Types.TIMESTAMP:
			type = "java.util.Date";
			break;
		case Types.BLOB:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			type = "byte[]";
			break;
		default:
			type = "java.lang.String";
		}
		return type;
	}

	public int getColumnCount()
	{
		return (fields == null) ? 0 : fields.size();
	}

	public int getColumnDisplaySize(int column)
	{
		try
		{
			Object val = getData(column, "column_size");
			return Integer.parseInt(val.toString());
		}
		catch (Exception e) { return 0; }
	}

	public String getColumnLabel(int column)
	{
		return getColumnName(column);
	}

	public String getColumnName(int column)
	{
		Object val = getData(column, "column_name");
		if (val == null)
			return null;
		else
			return val.toString();
	}

	public int getColumnType(int column)
	{
		try
		{
			Object val = getData(column, "data_type");
			return Integer.parseInt(val.toString());
		}
		catch (Exception e) { return 0; }
	}

	public String getColumnTypeName(int column)
	{
		String type;
		switch (getColumnType(column))
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

	public int getPrecision(int column)
	{
		try
		{
			Object val = getData(column, "column_precision");
			return Integer.parseInt(val.toString());
		}
		catch (Exception e) { return 0; }
	}

	public int getScale(int column)
	{
		try
		{
			Object val = getData(column, "column_scale");
			return Integer.parseInt(val.toString());
		}
		catch (Exception e) { return 0; }
	}

	public String getSchemaName(int column)
	{
		return "";
	}

	public String getTableName(int column)
	{
		return table;
	}

	public boolean isAutoIncrement(int column)
	{
		return false;
	}

	public boolean isCaseSensitive(int column)
	{
		return true;
	}

	public boolean isCurrency(int column)
	{
		return false;
	}

	public boolean isDefinitelyWritable(int column)
	{
		return false;
	}

	public int isNullable(int column)
	{
		return columnNullableUnknown;
	}

	public boolean isReadOnly(int column)
	{
		return true;
	}

	public boolean isSearchable(int column)
	{
		return true;
	}

	public boolean isSigned(int column)
	{
		switch (getColumnType(column))
		{
		case Types.DECIMAL:
		case Types.DOUBLE:
		case Types.FLOAT:
		case Types.NUMERIC:
		case Types.REAL:
		case Types.BIGINT:
		case Types.INTEGER:
		case Types.SMALLINT:
		case Types.TINYINT:
			return true;
		case Types.BIT:
		case Types.BOOLEAN:
		case Types.DATE:
		case Types.TIME:
		case Types.TIMESTAMP:
		case Types.BLOB:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
		default:
			return false;
		}
	}

	public boolean isWritable(int column) 
	{
		return false;
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



