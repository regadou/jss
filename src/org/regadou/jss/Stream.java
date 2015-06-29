package org.regadou.jss;

import java.io.*;
import java.net.*;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import org.mozilla.javascript.*;


public class Stream extends ScriptableObject implements Runnable
{
	private File file = null;
   private URL url = null;
	private String commandline = null;
   private Socket net = null;
   private ServerSocket server = null;
   private InputStream input = null;
	private OutputStream output = null;
	private Writer writer = null;
	private Process process = null;
   private Thread listening = null;
   private Function function = null;
	private String charset = "ISO-8859-1";
	private byte[] buffer = null;
	private int bufferSize = 1024, initLength = -1, leftToRead = -1;
	private long lastMod = 0;
	public static final char PIPECHAR = '|';
	public static final int INPUT=1, OUTPUT=2;
   public static final String OPERATIONS[] = {null, "input", "output"};
   public static final String FUNCTIONS[] = {"listen", "hasData", "read", "readLine", "readAll", "readString",
                                             "skip", "write", "writeString", "copyTo", "open", "close", "lastModified",
															"isDirectory", "list", "toSource", "setBuffer", "getSource", "setLength",
															"setCharset", "getCompleted", "getInputStream", "getOutputStream"};
	public static final String TOPLEVEL[] =
	{
		"arpa","root","aero","biz","cat","com","coop","edu","gov","info",
		"int","jobs","mil","mobi","museum","name","net","org","pro","tel","travel"
	};

   public Stream(Object src)
   {
		if (src == null)
			throw new RuntimeException("Invalid stream parameter");
		else if (src instanceof Scriptable)
		{
			Scriptable s = (Scriptable)src;
			if (s instanceof Wrapper)
				src = ((Wrapper)s).unwrap();
		}
      if (src instanceof Stream)
         src = ((Stream)src).getSource();

		if (src instanceof File)
		{
			try { file = ((File)src).getCanonicalFile(); }
			catch (Exception e) { file = (File)src; }
		}
		else if (src instanceof URL)
			url = (URL)src;
		else if (src instanceof Socket)
			net = (Socket)src;
		else if (src instanceof ServerSocket)
			server = (ServerSocket)src;
		else if (src instanceof Number)
		{
			try { server = new ServerSocket( ((Number)src).intValue() ); }
			catch (Exception e) { throw new RuntimeException(e); }
		}
		else if (src instanceof InputStream)
			input = (InputStream)src;
		else if (src instanceof OutputStream)
			output = (OutputStream)src;
		else if (src instanceof Writer)
			writer = (Writer)src;
		else
		{
			String txt = src.toString().trim();
			if (txt.charAt(0) == PIPECHAR || txt.charAt(txt.length()-1) == PIPECHAR)
				commandline = txt;
			else
			{
				String params[] = connectAddress(txt);
				if (params != null)
				{
					try { net = new Socket(params[0], Integer.parseInt(params[1])); }
					catch (Exception e) { throw new RuntimeException(e); }
				}
				else
				{
					try { url = new URL(txt); }
					catch (Exception e) { file = new File(txt); }
				}
			}
		}
      defineFunctionProperties(FUNCTIONS, Stream.class, ScriptableObject.DONTENUM);
		Interpreter.register(this);
	}

   public String toString()
   {
      return "[Stream "+getSourceString()+"]";
   }

   public String toSource()
   {
      String txt = "(new Stream(";
      if (net != null)
         txt += "\""+net.getInetAddress()+":"+net.getPort()+"\"";
      else if (server != null)
         txt += server.getLocalPort();
      else if (file != null)
         txt += "new File("+file+")";
      else if (url != null)
         txt += "new URL("+url+")";
      else if (commandline != null)
         txt += "'"+commandline+"'";
      else if (input != null)
         txt += "new "+input.getClass().getName()+"()";
      else if (output != null)
         txt += "new "+output.getClass().getName()+"()";
      return txt+"))";
   }

   public String getClassName()
   {
      return "Stream";
   }

   public Object getDefaultValue(Class hint)
   {
      return toString();
   }

	public InputStream getInputStream()
	{
		if (input == null && !open("r"))
			return null;
		else if (buffer != null && buffer.length > 0)
			throw new RuntimeException("Inconsistancy probable with a Stream that has buffered data");
		else
			return input;
	}

	public OutputStream getOutputStream()
	{
		if (output == null && !open("w"))
			return null;
		return output;
	}

	public int setBuffer(int size)
	{
		if (size > 0)
			bufferSize = size;
		return bufferSize;
	}

	public String setCharset(String cs)
	{
		if (cs != null && !cs.equals(""))
			charset = cs;
		return charset;
	}

	public int setLength(int len)
	{
		if (isDirectory())
			return 0;
		else if (len >= 0)
			initLength = leftToRead = len;
		return leftToRead;
	}

	public boolean isDirectory()
	{
		if (file == null)
			return false;
		else
			return file.isDirectory();
	}

	public Scriptable list()
	{
		if (isDirectory())
		{
			String files[] = file.list();
			List lst = new ArrayList();
			for (int i = 0; i < files.length; i++)
				lst.add(files[i]);
			return Interpreter.newObject(lst);
		}
		else
			return null;
	}

	public long lastModified()
	{
		if (lastMod > 0)
			;
		else if (file != null)
		{
			lastMod = file.lastModified();
			if (lastMod <= 0)
				return 0;
		}
		else if (url != null)
		{
			if (input != null)
				return 0;
			if (!open("r") || lastMod <= 0)
				return 0;
		}
		else
			return 0;

		return lastMod;
	}

	public float getCompleted()
	{
		if (initLength < 0)
			return -1;
		else if (initLength == 0)
			return 1;
		else if (leftToRead >= 0)
			return (float)(initLength - leftToRead) / initLength;
		else if (input == null)
			return -1;
		else
		{
			try
			{
				int n = input.available();
				return (float)(initLength - n) / initLength;
			}
			catch (Exception e)
			{
				return -1;
			}
		}
	}

	public boolean open(String mode)
	{
		if (server != null)
			throw new RuntimeException("Cannot open a server socket stream");
		else if (isDirectory())
			throw new RuntimeException("Cannot open a directory stream");
		else if (mode == null)
			mode = "";
		else
			mode = mode.toLowerCase();
		boolean toread=false, towrite=false;
		if (mode.indexOf("r") >= 0)
			toread = true;
		if (mode.indexOf("w") >= 0)
			towrite = true;
		if (!toread && !towrite)
			toread = towrite = true;
		if (commandline != null && input == null && output == null)
			return openCommand(toread, towrite);
		if (toread && input == null)
		{
			try
			{
				if (file != null)
					input = new FileInputStream(file);
				else if (url != null)
				{
					URLConnection c = url.openConnection();
					lastMod = c.getLastModified();
					input = c.getInputStream();
				}
				else if (net != null)
			      input = net.getInputStream();
			}
			catch (Exception e) { new RuntimeException(e); }
			if (input == null)
				throw new RuntimeException("Cannot read from stream "+this);
		}
		if (towrite && output == null)
		{
			try
			{
				if (writer != null)
					return true;
				if (file != null)
					output = new FileOutputStream(file);
				else if (net != null)
			      output = net.getOutputStream();
			}
			catch (Exception e) { new RuntimeException(e); }
			if (output == null)
				throw new RuntimeException("Cannot write to stream "+this);
		}
		return true;
	}

   public boolean hasData()
   {
   	if (server != null)
   		return (listening != null);
		if (!open("r"))
			return false;
		try { return ((buffer != null && buffer.length > 0)
						|| leftToRead > 0
						|| input.available() > 0); }
		catch (Exception e) { return false; }
   }

   public void run()
   {
      Scriptable global = null;
      Context cx = null;
		InputStream error = null;
      while (listening != null)
      {
         try
         {
				if (process != null)
				{
					if (error == null)
						error = process.getErrorStream();
					int n = error.available();
					if (n > 0)
					{
						byte b[] = new byte[n];
						error.read(b);
						System.out.println(new String(b));
					}
					else
						listening.sleep(100);
				}
				else if (server != null)
				{
	            Socket s = server.accept();
	            if (function != null && s != null)
	            {
						if (global == null)
							global = getTopLevelScope(this);
						if (cx == null)
						{
							cx = Context.enter();
							cx.putThreadLocal("interpreter", this);
						}
	               function.call(cx, global, global, new Object[]{new Stream(s)});
	            }
					else
					{
						listening = null;
						net = s;
					}
				}
				else
					listening = null;
         }
         catch (Exception e)
         {
	         if (function != null)
	         {
					if (global == null)
						global = getTopLevelScope(this);
					if (cx == null)
					{
						cx = Context.enter();
						cx.putThreadLocal("interpreter", this);
					}
	            function.call(cx, global, global, new Object[]{e.toString()});
	         }
	         else
				   throw new RuntimeException(e);
         }
      }

      if (cx != null)
      	Context.exit();
   }

   public boolean listen(Object obj)
   {
      if (server == null)
         throw new RuntimeException("Cannot use listen on a non-server stream");
      else if (listening != null)
       {
         if (obj == null)
         {
            listening = null;
            function = null;
            return true;
         }
         else
            return false;
      }
      else if (!(obj instanceof Function))
		{
			listening = new Thread(this);
			run();
         return true;
		}

      function = (Function)obj;
      listening = new Thread(this);
      listening.start();
      return true;
   }

   public byte[] peek(int nb)
   {
		if (nb <= 0 || !hasData())
			return new byte[0];
      try
      {
      	while (buffer == null || buffer.length < nb)
      	{
	         int na = (leftToRead > 0) ? leftToRead : input.available();
	         if (na <= 0)
	         	break;
				int ns = (buffer == null) ? 0 : buffer.length;
				int len = na + ns;
				if (len > nb)
					len = nb;
				if (len > ns)
				{
					if (buffer == null)
						buffer = new byte[len];
					else
					{
						byte b[] = new byte[len];
						System.arraycopy(buffer, 0, b, 0, ns);
						buffer = b;
					}
					int got = input.read(buffer, ns, len-ns);
					if (got < 0)
						break;
					if (leftToRead > 0)
						leftToRead -= got;
					len = ns + got;
					if (len != buffer.length)
					{
						byte b[] = new byte[len];
						System.arraycopy(buffer, 0, b, 0, len);
						buffer = b;
					}
				}
			}
			if (buffer == null)
				return new byte[0];
			else
			{
				if (nb > buffer.length)
					nb = buffer.length;
				byte bytes[] = new byte[nb];
				System.arraycopy(buffer, 0, bytes, 0, nb);
				return bytes;
			}
      }
      catch (Exception e)
		{ throw new RuntimeException(e); }
   }

   public int skip(int nb)
   {
		if (nb <= 0 || !hasData())
			return 0;
      try
      {
			int start = 0;
			if (buffer != null)
			{
				int ns = buffer.length;
				if (nb >= ns)
					buffer = null;
				else
				{
					byte b[] = new byte[ns-nb];
					System.arraycopy(buffer, nb, b, 0, ns-nb);
					buffer = b;
					ns = nb;
				}
				start = ns;
				nb -= ns;
			}
			while (nb > 0)
			{
	         int na = (leftToRead > 0) ? leftToRead : input.available();
	         if (na <= 0)
	         	break;
				if (na < nb)
					nb = na;
				int got = (int)input.skip(nb);
				if (got < 0)
					break;
				if (leftToRead > 0)
					leftToRead -= got;
				start += got;
				nb -= got;
			}
			return start;
      }
      catch (Exception e)
		{ throw new RuntimeException(e); }
   }

   public byte[] readBytes(int nb)
   {
		if (nb <= 0 || !hasData())
			return null;
      try
      {
			byte bytes[] = null;
			int start = 0;
			if (buffer != null)
			{
				int ns = buffer.length;
				if (nb >= ns)
				{
					bytes = buffer;
					buffer = null;
				}
				else
				{
					bytes = new byte[nb];
					System.arraycopy(buffer, 0, bytes, 0, nb);
					byte b[] = new byte[ns-nb];
					System.arraycopy(buffer, nb, b, 0, ns-nb);
					buffer = b;
					ns = nb;
				}
				start = ns;
				nb -= ns;
			}
			while (nb > 0)
			{
	         int na = (leftToRead > 0) ? leftToRead : input.available();
	         if (na <= 0)
	         	break;
				if (na < nb)
					nb = na;
				if (bytes == null)
					bytes = new byte[nb];
				else if (bytes.length != start+nb)
				{
					byte b[] = new byte[start+nb];
					System.arraycopy(bytes, 0, b, 0, start);
					bytes = b;
				}
				int got = input.read(bytes, start, nb);
				if (got < 0)
					break;
				if (leftToRead > 0)
					leftToRead -= got;
				start += got;
				nb -= got;
			}
			return (bytes == null) ? (new byte[0]) : bytes;
      }
      catch (Exception e)
		{ throw new RuntimeException(e); }
   }

   public String readLine()
   {
		if (!hasData())
			return null;
		byte bytes[];
		String txt = "";
		do
		{
			bytes = peek(bufferSize);
			if (bytes == null || bytes.length == 0)
				break;
			for (int i = 0; i < bytes.length; i++)
			{
				char c = (char)(bytes[i]);
				int got = 0;
				switch (c)
				{
				case '\r':
					got++;
					if (i+1 >= bytes.length || bytes[i+1] != '\n')
						break;
				case '\n':
					got++;
					break;
				default:
				}
				if (got > 0)
				{
					try
					{
						if (i > 0)
							txt += new String(readBytes(i), charset);
						readBytes(got);
						return txt;
					}
					catch (Exception e) { throw new RuntimeException(e); }
				}
			}
			try { txt += new String(readBytes(bytes.length), charset); }
			catch (Exception e) { throw new RuntimeException(e); }
		}
		while (bytes != null);
		return txt;
	}

   public byte[] readUntil(byte end[])
   {
		if (end == null || end.length == 0)
			return new byte[0];
		byte bytes[] = null, tmp[];
		int i, j, len;
		do
		{
			len = bufferSize;
			tmp = peek(len);
			if (tmp == null || tmp.length < end.length)
				break;
			for (i = 0; i < tmp.length; i++)
			{
				if (tmp[i] != end[0])
					continue;
				else if (tmp.length-i < end.length)
				{
					len = i + end.length;
					tmp = peek(len);
					if (tmp == null || tmp.length != len)
						break;
				}
				for (j = 1; j < end.length; j++)
					if (tmp[i+j] != end[j])
						break;
				if (j >= end.length)
				{
					tmp = (i==0) ? new byte[0] : readBytes(i);
					if (bytes == null)
						bytes = tmp;
					else
					{
						byte b[] = bytes;
						bytes = new byte[b.length+tmp.length];
						System.arraycopy(b, 0, bytes, 0, b.length);
						System.arraycopy(tmp, 0, bytes, b.length, tmp.length);
					}
					readBytes(end.length);
					return bytes;
				}
			}
			if (bytes == null)
				bytes = tmp;
			else
			{
				byte b[] = bytes;
				bytes = new byte[b.length+tmp.length];
				System.arraycopy(b, 0, bytes, 0, b.length);
				System.arraycopy(tmp, 0, bytes, b.length, tmp.length);
			}
			readBytes(tmp.length);
			if (i < tmp.length)
				break;
		}
		while (tmp != null);
		if (bytes == null)
			bytes = new byte[0];
		return bytes;
	}

   public int readUntil(byte end[], OutputStream dst) throws IOException
   {
		return readUntil(end, dst, 0);
	}

   public int readUntil(byte end[], OutputStream dst, int delay) throws IOException
   {
		if (end == null || end.length == 0)
			return 0;
		byte tmp[];
		int i, j, len, bytes=0;
		do
		{
			len = bufferSize;
			try{if (delay > 0)Thread.currentThread().sleep(delay);}
			catch(Exception e){}
			tmp = peek(len);
			if (tmp == null)
				break;
			else if (tmp.length < end.length)
			{
				dst.write(tmp);
				readBytes(tmp.length);
				bytes += tmp.length;
			}
			for (i = 0; i < tmp.length; i++)
			{
				if (tmp[i] != end[0])
					continue;
				else if (tmp.length-i < end.length)
				{
					try{if (delay > 0)Thread.currentThread().sleep(delay);}
					catch(Exception e){}
					len = i + end.length;
					tmp = peek(len);
					if (tmp == null || tmp.length != len)
						break;
				}
				for (j = 1; j < end.length; j++)
					if (tmp[i+j] != end[j])
						break;
				if (j >= end.length)
				{
					dst.write(tmp, 0, i);
					bytes += i;
					readBytes(i+end.length);
					return bytes;
				}
			}
			if (tmp != null)
			{
				dst.write(tmp);
				readBytes(tmp.length);
				bytes += tmp.length;
				if (i < tmp.length)
					break;
			}
		}
		while (tmp != null);
		return bytes;
	}

   public Scriptable readAll()
   {
		ArrayList lst = new ArrayList();
		String txt = "";
		while (txt != null)
		{
			txt = readLine();
			if (txt != null)
				lst.add(txt);
      }
      return Interpreter.newObject(lst.toArray());
   }

   public String readString()
   {
		if (!open("r"))
			return null;
      try
      {
			String txt = (buffer == null) ? "" : new String(buffer);
			buffer = null;
			int na = 0;
         do
         {
				na = (leftToRead > 0) ? leftToRead : input.available();
            if (na > 0)
            {
               byte b[] = new byte[na];
               int got = input.read(b);
					if (got < 0)
						break;
					if (leftToRead > 0)
						leftToRead -= got;
               txt += new String(b, 0, got, charset);
            }
         } while (na > 0);
			return txt;
		}
		catch (Exception e)
		{ throw new RuntimeException(e); }
	}

   public String read(int nb)
   {
		byte b[] = readBytes(nb);
		if (b == null)
			return null;
		else
		{
			try { return new String(b, charset); }
			catch (Exception e) { throw new RuntimeException(e); }
		}
	}

   public void write(String txt)
   {
		if (txt == null || txt.equals("") || !open("w"))
			return;
      try
      {
			if (output != null)
			{
				output.write(txt.getBytes());
	 			output.flush();
			}
			else if (writer != null)
			{
				writer.write(txt);
	 			writer.flush();
			}
      }
      catch (Exception e) { throw new RuntimeException(e); }
   }

   public void writeString(String txt)
   {
		if (txt == null || txt.equals("") || !open("w"))
			return;
      try
      {
			if (output != null)
			{
				output.write(txt.getBytes(charset));
	 			output.flush();
			}
			else if (writer != null)
			{
				writer.write(txt);
	 			writer.flush();
			}
      }
      catch (Exception e) { throw new RuntimeException(e); }
   }

   public void writeBytes(byte b[])
   {
   	if (b == null || b.length == 0 || !open("w"))
   		return;
      try
      {
			if (output != null)
			{
				output.write(b);
	 			output.flush();
			}
			else if (writer != null)
			{
				writer.write(new String(b));
	 			writer.flush();
			}
      }
      catch (Exception e) { throw new RuntimeException(e); }
   }

   public void writeBytes(byte b[], int off, int len)
   {
		if (b == null || off < 0 || len <= 0 || (off+len) > b.length || !open("w"))
			return;
      try
      {
			if (output != null)
			{
				output.write(b, off, len);
	 			output.flush();
			}
			else if (writer != null)
			{
				writer.write((new String(b)).substring(off, len));
	 			writer.flush();
			}
      }
      catch (Exception e) { throw new RuntimeException(e); }
   }

   public int copyTo(Object dst) {
      if (dst == null)
         return 0;
      Stream that = (dst instanceof Stream) ? (Stream)dst : new Stream(dst);
      byte[] buffer = new byte[bufferSize];
      InputStream in = this.getInputStream();
      OutputStream out = that.getOutputStream();
      int copied = 0;
      try {
         for (int got = 0; got >= 0;) {
            got = in.read(buffer);
            if (got > 0) {
               out.write(buffer, 0, got);
               copied += got;
            }
         }
      }
      catch (Exception e) { throw new RuntimeException(e); }
      this.close();
      that.close();
      return copied;
   }

   public void close()
   {
      try
      {
			if (listening != null)
				listening = null;
         if (input != null)
			{
				input.close();
				input = null;
			}
         if (output != null)
			{
				output.flush();
				output.close();
				output = null;
			}
         if (writer != null)
			{
				writer.flush();
				writer.close();
				writer = null;
			}
			if (process != null)
			{
				process.destroy();
				process = null;
			}
         if (net != null)
			{
            net.close();
				net = null;
			}
         if (server != null)
			{
				server.close();
				server = null;
			}
			leftToRead = -1;
			lastMod = 0;
			buffer = null;
      }
      catch (Exception e) { throw new RuntimeException(e); }
      finally { Interpreter.unregister(this); }
   }

	public static boolean isStreamable(Object src)
	{
		if (src == null)
			return false;
		else if (src instanceof Scriptable)
		{
			Scriptable s = (Scriptable)src;
			if (s instanceof Wrapper)
				src = ((Wrapper)s).unwrap();
		}
		if (src instanceof String)
		{
			if (src.equals(""))
				return false;
			String txt = src.toString();
			if (txt.charAt(0) == PIPECHAR || txt.charAt(txt.length()-1) == PIPECHAR)
				return true;
		}
		return isStreamable(src, INPUT) || isStreamable(src, OUTPUT);
	}

	public static boolean isStreamable(Object src, int op)
	{
		if (src == null)
			return false;
		else if (src instanceof Stream)
			src = ((Stream)src).getSource();
		else if (src instanceof Scriptable)
		{
			Scriptable s = (Scriptable)src;
			if (s instanceof Wrapper)
				src = ((Wrapper)s).unwrap();
		}

		if (src instanceof String)
		{
			if (src.equals(""))
				return false;
			String txt = src.toString();
			if (txt.charAt(0) == PIPECHAR)
				return  (op == OUTPUT);
			if (txt.charAt(txt.length()-1) == PIPECHAR)
				return (op == INPUT);

			String params[] = connectAddress(txt);
			if (params != null)
				return true;
			try { src = new URL(txt); }
			catch (Exception e) { src = new File(txt); }
		}
		if ((src instanceof File) || (src instanceof Socket))
			return true;

		switch (op)
		{
		case INPUT:
			return ( (src instanceof URL) || (src instanceof ServerSocket)
					|| (src instanceof Number) || (src instanceof InputStream) );
		case OUTPUT:
			return ( (src instanceof Writer) || (src instanceof OutputStream) );
		default:
			throw new RuntimeException("Invalid Stream operation parameter");
		}
	}

	public Object getSource()
	{
		if (file != null)
			return file;
		else if (url != null)
			return url;
		else if (net != null)
			return net;
		else if (commandline != null)
			return commandline;
		else if (server != null)
			return server;
		else if (writer != null)
			return writer;
		else if (input != null)
			return input;
		else if (output != null)
			return output;
		else
			return null;
	}

   private String getSourceString() {
      if (net != null)
         return net.getInetAddress()+":"+net.getPort();
      else if (server != null)
         return "localhost@"+server.getLocalPort();
      else if (file != null)
         return file.toString();
      else if (url != null)
         return url.toString();
      else if (commandline != null)
         return commandline;
      else if (input != null)
         return input.getClass().getName();
      else if (output != null)
         return output.getClass().getName();
      else if (writer != null)
         return writer.getClass().getName();
      else
         return "?";
   }

   private static String[] connectAddress(String adr)
   {
      int p = adr.indexOf(':');
		if (p <= 0)
			return null;
		try
      {
			String port = adr.substring(p+1);
			int pno = Integer.parseInt(port);
			if (pno <= 0 || pno >= 0xffff)
				return null;
         String host = adr.substring(0,p).toLowerCase();
			String parts[] = host.split(".");
			if (parts.length == 1)
				return null;
			boolean isHost = false, isIP = false;
			for (p = parts.length-1; p >= 0; p--)
			{
				String part = parts[p];
				if (part.equals(""))
					return null;
				else if (isHost)
				{
					int len = part.length();
					for (int i = 0; i < len; i++)
					{
						char c = part.charAt(i);
						if (c == '-')
							continue;
						else if (c < '0' || c > 'z')
							return null;
						else if (c > '9' && c < 'a')
							return null;
					}
				}
				else if (isIP)
				{
					int n = Integer.parseInt(part);
					if (n < 0 || n > 255)
						return null;
				}
				else
				{
					try
					{
						int n = Integer.parseInt(part);
						if (n < 0 || n > 255 || p != 3)
							return null;
						isIP = true;
					}
					catch (Exception e)
					{
						if (part.length() == 2)
							isHost = true;
						else
						{
							for (int i = 0; i < TOPLEVEL.length; i++)
								if (TOPLEVEL[i].equals(part))
								{
									isHost = true;
									break;
								}
							if (!isHost)
								return null;
						}
					}
				}
			}
	      return new String[]{host, port};
      }
      catch (Exception e) { return null; }
   }

	private boolean openCommand(boolean toread, boolean towrite)
	{
		String cmd = commandline;
		int len = cmd.length();
		char first = cmd.charAt(0);
		char last = cmd.charAt(len-1);
		if (toread && last != PIPECHAR)
			throw new RuntimeException("Cannot read from stream "+this);
		if (towrite && first != PIPECHAR)
			throw new RuntimeException("Cannot write to stream "+this);
		if (last == PIPECHAR)
		{
			cmd = cmd.substring(0,len-1);
			toread = true;
		}
		if (first == PIPECHAR)
		{
			cmd = cmd.substring(1);
			towrite = true;
		}
		try
		{
			process = Runtime.getRuntime().exec(cmd);
			if (toread)
				input = process.getInputStream();
			if (towrite)
				output = process.getOutputStream();
			listening = new Thread(this);
      	listening.start();
      	Thread.currentThread().sleep(500);
			return true;
		}
		catch (Exception e) { throw new RuntimeException(e); }
	}
}

