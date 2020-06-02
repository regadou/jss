package org.regadou.jmdb;

import java.sql.*;
import java.io.File;
import java.util.Properties;
import java.util.logging.Logger;
import com.healthmarketscience.jackcess.*;

public class MDBDriver implements Driver
{
	private static MDBDriver driver = null;
	private static final int major=0, minor=1;
	private static final String urlHeader = "jdbc:access:";

	static
	{
	   try
	   {
			if (driver == null)
			{
	      	driver = new MDBDriver();
	      	DriverManager.registerDriver(driver);
//	      	System.setSecurityManager(new java.rmi.RMISecurityManager());
	      }
	   }
	   catch (Exception e)
	   {
	   	driver = null;
	   	new RuntimeException("Could not initialize MDBDriver: "+e);
	   }
	}

	public MDBDriver() throws SQLException {}

	public boolean acceptsURL(String url) throws SQLException
	{
		return (url != null && url.startsWith(urlHeader));
	}

	public Connection connect(String url, Properties info) throws SQLException
	{
		if (!acceptsURL(url))
			return null;
		try
		{
			url = url.substring(urlHeader.length());
			int p = url.indexOf("?");
			if (p >= 0)
				url = url.substring(0, p);
			File f = new File(url);
			Database db = Database.open(f);
			return new RegConnection(db);
		}
		catch (Exception e) { throw new SQLException(e.toString()); }
	}

	public int getMajorVersion()
	{
		return major;
	}

	public int getMinorVersion()
	{
		return minor;
	}

	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException
	{
		return new DriverPropertyInfo[0];
	}

	public boolean jdbcCompliant()
	{
		return false;
	}
	
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
	   throw new SQLFeatureNotSupportedException("MDBDriver does not use logging");
	}
}

