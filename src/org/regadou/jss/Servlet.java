package org.regadou.jss;

import java.util.*;
import java.io.*;
import javax.servlet.http.*;
import org.mozilla.javascript.*;
import java.text.SimpleDateFormat;


public class Servlet extends HttpServlet
{
	public int WAITTIME = 0, maxtries = 3, bufferSize = 1024;
	public static final String APPLICATION = "application", tryparam = "authentication",
										reserved[] = {APPLICATION,"request","response","args"};
	private static Map uploads = new HashMap();
	private Map permissions = null, application = null, threads = null, realms = null, parameters = null;
	private String userlog[] = null;

	private static final Map mimetypes = new HashMap();
	static {
      mimetypes.put("txt",    "text/plain");
      mimetypes.put("htm",    "text/html");
      mimetypes.put("html",   "text/html");
      mimetypes.put("xhtml",   "text/html");
      mimetypes.put("xml",    "text/xml");
      mimetypes.put("js",     "text/javascript");
      mimetypes.put("json",   "text/json");
      mimetypes.put("jsp",    "application/jsp");
      mimetypes.put("groovy", "text/groovy");
      mimetypes.put("ets",    "text/ets");
      mimetypes.put("csv",    "text/csv");
      mimetypes.put("xls",    "application/vnd.ms-excel");
      mimetypes.put("svg",    "image/svg+xml");
      mimetypes.put("png",    "image/png");
      mimetypes.put("jpg",    "image/jpeg");
      mimetypes.put("jpeg",   "image/jpeg");
      mimetypes.put("jpe",    "image/jpeg");
      mimetypes.put("gif",    "image/gif");
      mimetypes.put("bmp",    "image/x-ms-bmp");
      mimetypes.put("ppm",    "image/x-portable-pixmap");
      mimetypes.put("xpm",    "image/x-xpixmap");
      mimetypes.put("x3d",    "model/x3d+xml");
      mimetypes.put("x3dv",   "model/x3d+vrml");
      mimetypes.put("x3db",   "model/x3d+binary");
      mimetypes.put("wrl",    "model/vrml");
      mimetypes.put("vrml",   "model/vrml");
      mimetypes.put("3ds",    "model/3ds");
      mimetypes.put("dae",    "model/dae");
      mimetypes.put("obj",    "model/obj");
      mimetypes.put("mp3",    "audio/mp3");
      mimetypes.put("wav",    "audio/wav");
      mimetypes.put("mid",    "audio/midi");
      mimetypes.put("midi",   "audio/midi");
      mimetypes.put("mp4",    "video/mp4");
      mimetypes.put("mpg",    "video/mpg");
      mimetypes.put("flv",    "video/flv");
      mimetypes.put("swf",    "application/x-shockwave-flash");
      mimetypes.put("zip",    "application/zip");
      mimetypes.put("jar",    "application/zip");
      mimetypes.put("war",    "application/zip");
      mimetypes.put("bin",    "application/octet-stream");
      mimetypes.put("so",     "application/octet-stream");
      mimetypes.put("exe",    "application/octet-stream");
      mimetypes.put("dll",    "application/octet-stream");
      mimetypes.put("class",  "application/octet-stream");
	}

	public String toString()
	{
		return "JavaScriptServlet (JSS)";
	}

	public static Object getUploads() { return uploads; }

	public static Object getUploadStatus(String name)
	{
		Object val = null;
		if (name != null && !name.equals(""))
		{
			val = uploads.get(name);
			if (val instanceof Stream)
				val = new Float(((Stream)val).getCompleted());
			else if (val instanceof HttpInputStream)
				val = new Float(((HttpInputStream)val).getCompleted());
			else if (val instanceof Number)
				uploads.remove(name);
		}
		return val;
	}

	public String getServletInfo()
	{
		return "JSS javascript servlet by Regadou.net";
	}

	public void init() throws javax.servlet.ServletException
	{
		permissions = new java.util.HashMap();
		application = new java.util.HashMap();
		threads = new java.util.HashMap();

		String param = getInitParameter("init");
		if (param != null && !param.equals(""))
			execute(param);

		param = getInitParameter("waittime");
		if (param != null && !param.equals(""))
		{
			try { WAITTIME = Integer.parseInt(param); }
			catch (Exception e) {}
		}

		param = getInitParameter("userlog");
		if (param != null && !param.equals(""))
		{
			String path = getServletContext().getRealPath(param);
			if (path == null)
			{
				log("Could not init userlog "+param);
				return;
			}
			userlog = new String[2];
			int p = path.indexOf("*");
			if (p < 0)
			{
				userlog[0] = path;
				userlog[1] = null;
			}
			else
			{
				userlog[0] = path.substring(0,p);
				userlog[1] = path.substring(p+1);
			}
		}
	}

	public void destroy()
	{
		String param = getInitParameter("destroy");
		if (param != null && !param.equals(""))
			execute(param);
	}

	public void execute(String path)
	{
		String script = getServletContext().getRealPath(path);
		Interpreter js = new Interpreter(this);
		js.defineProperty(APPLICATION, Interpreter.newObject(application, js), 0);
		js.load(script);
		Scriptable app = (Scriptable)js.get(APPLICATION,js);
		Object names[] = app.getIds();
		for (int i = 0; i < names.length; i++)
		{
			String name = names[i].toString();
			Object val = app.get(name, app);
			if (val != null)
				application.put(name, val);
		}
      Interpreter.terminate(js);
	}

   public void doGet(HttpServletRequest httpRequest, HttpServletResponse httpResponse) throws IOException
   {
		doRequest(httpRequest, httpResponse);
	}

   public void doPost(HttpServletRequest httpRequest, HttpServletResponse httpResponse) throws IOException
   {
		doRequest(httpRequest, httpResponse);
	}

   public void doRequest(HttpServletRequest httpRequest, HttpServletResponse httpResponse) throws IOException
   {
		Interpreter js = null;
      try
      {
			String script = httpRequest.getPathTranslated();
			if (script == null || script.equals(""))
			{
				String base = httpRequest.getContextPath();
				String src = httpRequest.getRequestURI();
				if (!base.equals("") && src.startsWith(base))
					src = src.substring(base.length());
				script = getServletContext().getRealPath(src);
			}

			File file = new File(script);
			if (file.isDirectory())
			{
				File[] files = file.listFiles();
				for (int i = 0; i < files.length; i++) {
					String[] path = files[i].toString().split("/");
					String[] parts = path[path.length-1].split("\\.");
					if (parts.length == 2 && parts[0].toLowerCase().equals("index")) {
						file = files[i];
						break;
					}
				}
			}

			if (file.isDirectory()) {
			   httpResponse.setContentType("text/html");
			   OutputStream output = httpResponse.getOutputStream();
			   SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			   String title = "Index of "+httpRequest.getRequestURI();
			   String line = "<tr><th colspan='3'><hr></th></tr>\n";
			   String html = "<html><head><title>"+title+"</title></head>\n<body><h1>"+title+"</h1><table border='0' cellpadding='5' cellspacing='5'>\n<tr><th>Name</th><th>Size</th><th>Last modified</th></tr>\n"+line;
				output.write(html.getBytes());

				File[] files = file.listFiles();
				for (int i = 0; i < files.length; i++) {
					file = files[i];
					String date = format.format(new Date(file.lastModified()));
					html = "<tr><td><a href='"+file.getName()+"'>"+file.getName()+"</a></td><td align='right'>"+file.length()+"</td><td>"+date+"</td></tr>\n";
					output.write(html.getBytes());
				}

				html = line + "</table></body></html>\n";
				output.write(html.getBytes());
			}
			else if (file.toString().toLowerCase().endsWith(".jss")) {
			   js = new Interpreter(this);
			   js.defineProperty(APPLICATION, Interpreter.newObject(application, js), 0);
			   js.put("request", js, Interpreter.newObject(httpRequest, js));
			   js.put("response", js, Interpreter.newObject(httpResponse, js));
			   String query = httpRequest.getQueryString();

			   if (query == null)
				   ;
			   else if (query.indexOf("=") > 0)
				   setVariables(js, query);
			   else if (!query.equals(""))
			   {
			     Scriptable jsargs = (Scriptable)js.get("args",js);
			     String lst[] = query.split("\\+");
			     for (int i = 0; i < lst.length; i++)
				     jsargs.put(i, jsargs, unescape(lst[i]));
			   }

			   if (httpRequest.getMethod().toLowerCase().equals("post"))
				   getPostData(js, httpRequest, httpResponse);
			   httpResponse.setContentType("text/html");
			   String cache = getInitParameter("cache");
			   if (cache != null && cache.toLowerCase().charAt(0) == 'n')
				   httpResponse.setHeader("Cache-Control","no-cache");

			   js.load(file.toString());
			}
			else {
				String[] parts = file.toString().split("\\.");
				Object mimetype = mimetypes.get(parts[parts.length-1].toLowerCase());
				if (mimetype == null)
					mimetype = "text/plain";
			   httpResponse.setContentType(mimetype.toString());

			   byte[] buffer = new byte[bufferSize];
			   InputStream input = new FileInputStream(file);
			   OutputStream output = httpResponse.getOutputStream();
			   for (int got = 0; got >= 0;) {
			      got = input.read(buffer);
			      if (got > 0)
			         output.write(buffer, 0, got);
			   }
			   try { input.close(); }
			   catch (Exception e) {}
			}
      }
      catch (Exception e)
      {
			if (Interpreter.getException(e) != null) {
			   httpResponse.setContentType("text/html");
		      ByteArrayOutputStream baos = new ByteArrayOutputStream();
		      PrintWriter pw = new PrintWriter(baos);
		      e.printStackTrace(pw);
		      pw.flush();
				String msg = "<html><body><pre>\nException with servlet execution:\n\n"
							   +(new String(baos.toByteArray()))+"</pre></body></html>";
				if (js == null)
					httpResponse.getOutputStream().write(msg.getBytes());
				else
					js.write(msg);
				log(e.toString());
			}
      }

      Interpreter.terminate(js);
   }

	protected void setVariables(Interpreter js, String txt)
	{
		String params[] = txt.split("&");
		Map variables = new HashMap();
		for (int i = 0; i < params.length; i++)
		{
			String param = params[i];
			int pos = param.indexOf("=");
			if (pos < 1)
				continue;
			String name = unescape(param.substring(0,pos));
			txt = unescape(param.substring(pos+1));
			setValue(js, name, txt, variables);
		}
	}

	protected void setValue(Interpreter js, String name, Object data)
	{
		setValue(js, name, data, null);
	}

	protected void setValue(Interpreter js, String name, Object data, Map variables)
	{
		String type = null;
		int pos = name.indexOf("(");
		if (pos > 0)
		{
			type = name.substring(pos+1).toLowerCase();
			name = name.substring(0,pos);
			pos = type.indexOf(")");
			if (pos > 0)
				type = type.substring(0, pos);
			if (data instanceof File)
			{
				Stream s = new Stream(data);
				data = s.readString();
				s.close();
			}
		}
		String props[] = name.split("\\.");
		Scriptable parent = js;
		for (int p = 0; p < props.length; p++)
		{
			name = props[p];
			if (p == 0 && isReserved(name))
				break;
			Object val = null;
			if (p == props.length-1)
			{
				if (type == null)
					val = data;
				else if (type.equals("number"))
				{
					try
					{
						String txt = data.toString();
						if (txt.indexOf(".") >= 0)
							val = new Double(txt);
						else
							val = new Integer(txt);
					}
					catch (Exception e) { val = new Integer(0); }
				}
				else if (type.equals("boolean"))
				{
					switch (data.toString().charAt(0))
					{
					case 'F':
					case 'f':
					case 'N':
					case 'n':
					case '0':
						val = new Boolean(false);
						break;
					case 'T':
					case 't':
					case 'V':
					case 'v':
					case 'O':
					case 'o':
					case 'Y':
					case 'y':
					case '1':
						val = new Boolean(true);
						break;
					default:
						continue;
					}
				}
				else if (type.equals("date") && data != null)
				{
					if (data.equals(""))
						val = null;
					else if (data.toString().toLowerCase().equals("now"))
						val = new Date();
					else
					{
						long t = Interpreter.parseDate(data.toString());
						val = new Date(t);
					}
				}
				else
					val = data;
				if (variables != null)
				{
					Object old = variables.get(name);
					if (old != null)
					{
						Scriptable lst = null;
						if (old instanceof Scriptable)
						{
							Scriptable s = (Scriptable)old;
					      Object obj = s.get("length", s);
					      if (obj instanceof Number)
							{
						      int len = ((Number)obj).intValue();
								s.put(len, s, val);
								lst = s;
							}
						}
						if (lst == null)
						{
							Object a[] = {old, val};
							lst = Interpreter.newObject(a, js);
						}
						val = lst;
					}
					variables.put(name, val);
				}
				parent.put(name, parent, val);
			}
			else if (parent.has(name, parent))
			{
				val = parent.get(name, parent);
				if (!(val instanceof Scriptable))
				{
					val = Interpreter.newObject(null, js);
					parent.put(name, parent, val);
				}
				parent = (Scriptable)val;
			}
			else
			{
				val = Interpreter.newObject(null, js);
				parent.put(name, parent, val);
				parent = (Scriptable)val;
			}
		}
	}

	private void getPostData(Interpreter js, HttpServletRequest req, HttpServletResponse rsp)
	{
		String len = req.getHeader("Content-length");
		if (len == null)
			return;
		int n = Integer.parseInt(len);
		if (n < 1)
			return;
		String fulltype = req.getHeader("Content-type");
		String type = fulltype;
		if (type == null || type.equals(""))
			throw new RuntimeException("No mime type specified in POST data");
		else
		{
			int p = type.indexOf(";");
			if (p > 0)
			{
				String part = type.substring(p+1);
				type = type.substring(0,p);
			}
		}

		Stream st;
		InputStream in;
		try
		{
			in = req.getInputStream();
			st = new Stream(in);
			st.setLength(n);
		}
		catch (Exception e) { throw new RuntimeException(e.toString()); }
		if (type.equals("application/x-www-form-urlencoded"))
			setVariables(js, st.readString());
		else if (type.equals("multipart/form-data"))
		{
			Object val = js.get("upload", js);
			String txt = (val == null) ? "" : val.toString();
			HttpInputStream hs = new HttpInputStream(txt, fulltype, in, n, js);
			if (val != null && !val.equals(""))
				uploads.put(val, hs);
			hs.load();
			if (val != null && !val.equals(""))
				uploads.put(val, new Float(1));
		}
		else
			throw new RuntimeException("Unsupported mime type "+type+" for POST data");

		return;
	}

	private boolean isReserved(String name)
	{
		for (int i = 0; i < reserved.length; i++)
			if (name.equals(reserved[i]))
				return true;
		for (int i = 0; i < Interpreter.FUNCTIONS.length; i++)
			if (name.equals(Interpreter.FUNCTIONS[i]))
				return true;
		return false;
	}

	protected String extractData(String txt, String varname)
	{
		int p = txt.indexOf(varname);
		if (p < 0 || txt.charAt(p+varname.length()) != '=')
			return null;
		txt = txt.substring(p+varname.length()+1);
		if (txt.charAt(0) == '\"' && txt.charAt(txt.length()-1) == '\"')
			txt = txt.substring(1,txt.length()-1);
		return txt;
	}

	public static final String URLENCODE = "$&+,/:;=?@<>#%{}[]|^`'\\\"";
	public static String escape(String src)
	{
		if (src == null || src.equals(""))
			return src;
		int len = src.length();
		String dst = "";
		for (int i = 0; i < len; i++)
		{
			char c = src.charAt(i);
			if (c <= ' ' || c >= '~' || URLENCODE.indexOf(c) >= 0)
				dst += "%"+Integer.toString(c+0x100,16).substring(1);
			else
				dst += c;
		}
		return dst;
	}

	public static String unescape(String src)
	{
		if (src == null || src.equals(""))
			return src;
		src = src.replace('+', ' ');
		int len = src.length();
		String dst = "";
		for (int at = 0; at < len;)
		{
			int pos = src.indexOf('%', at);
			if (pos < 0)
			{
				dst += src.substring(at);
				at = len;
			}
			else
			{
				char ch = (char)Integer.parseInt(src.substring(pos+1,pos+3), 16);
				dst += src.substring(at, pos)+ch;
				at = pos + 3;
			}
		}
		return dst;
	}

	public Scriptable newScript(String name, Object src)
	{
		if (name == null || name.equals(""))
			throw new RuntimeException("Invalid script name");
		Object old = threads.get(name);
		if (old != null)
		{
			ScriptThread t = (ScriptThread)old;
			Interpreter js = t.getGlobal();
			if (js != null)
			{
				try { js.exit(); }
				catch (Exception e) {}
				t.stop();
			}
			threads.remove(name);
		}
		if (src == null || src.equals(""))
			return null;
		ScriptThread t = new ScriptThread(src, this, application, name);
		threads.put(name, t);
		t.start();
		try { Thread.currentThread().sleep(50); }
		catch (Exception e) {}
		Scriptable obj = t.getGlobal();
		return obj;
	}

	public Scriptable getScript(String name)
	{
		if (name == null || name.equals(""))
			throw new RuntimeException("Invalid script name");
		Object obj = threads.get(name);
		if (obj == null)
			return null;
		ScriptThread t = (ScriptThread)obj;
		return t.getGlobal();
	}

	public boolean killScript(String name)
	{
		if (name == null || name.equals(""))
			throw new RuntimeException("Invalid script name");
		ScriptThread t = (ScriptThread)threads.get(name);
		if (t == null)
			return false;
      threads.remove(name);
		t.interrupt();
		return true;
	}

	public String login(Interpreter js, Object userdata, String realm, String redirect, boolean crypted)
	{
		if (js == null || userdata == null  || realm == null || realm.equals(""))
			return null;
      HttpServletRequest request = null;
      HttpServletResponse response = null;
      Object val = Interpreter.getObject(js.get("request", js));
		if (val instanceof HttpServletRequest)
			request = (HttpServletRequest)val;
      val = Interpreter.getObject(js.get("response", js));
		if (val instanceof HttpServletResponse)
			response = (HttpServletResponse)val;
		if (request == null || response == null)
         return null;

		int code = 0;
		String msg = null, user = null;
		String auth = request.getHeader("authorization");
		if (auth == null)
		{
			code = 401;
			msg = "Authentication required";
			redirect = null;
		}
		else
		{
			String parts[] = auth.split(" ");
			if (parts.length < 2)
			{
				code = 401;
				msg = "Authentication received has no specified mode";
			}
			else
			{
				String mode = parts[0].toUpperCase();
				String credential[] = Interpreter.decodeBase64(parts[1]+"").split(":");
				user = (credential.length > 1) ? credential[0] : "";
				String pass = (credential.length > 1) ? credential[1] : "";
//msg="user=\""+user+"\" pass=\""+pass+"\"";
//response.getWriter().println("<script>alert('"+msg+"')</script>\n");
				if (!mode.equals("BASIC"))
				{
					code = 401;
					msg = mode+" mode not supported";
				}
				else if (user == null || user.equals(""))
				{
					code = 403;
					msg = "Forbidden access";
				}
				else
				{
					Map realmobj = getuserslist(userdata, realm, crypted);
					if (realmobj != null)
					{
						if (Boolean.getBoolean(realmobj.get("crypted").toString()))
							pass = Interpreter.crypt(pass, null);
						List lst = (List)(realmobj.get("users"));
						int nb = lst.size();
						for (int u = 0; u < nb; u++)
						{
							Map uobj = (Map)(lst.get(u));
							if (!user.equals(uobj.get("user")))
								continue;
							else if (!pass.equals(uobj.get("pass")))
								break;
							else
							{
								if (userlog != null)
									sendStatus(request, response, user, 200, "Login succesfull", realm);
								return user;
							}
						}
					}

					code = 401;
					msg = "Forbidden access for "+user;
					redirect = null;
				}
			}
		}

		sendStatus(request, response, user, code, msg, realm, redirect);
		js.eval("exit();", "failed login");
		return null;
	}

	/********************
	This function will return a realm object which should contain the following fields:
		file: the source file where to get the user list or null if none
		realm: the realm name
		crypted: true if passwords are crypted else false (text values)
		time: a long value representing seconds since january first 1970
		users: the list of valid users, which each being a {user,pass} map object

	*********************/
	public Map getuserslist(Object userdata, String realm, boolean crypted)
	{
		Map robj = null;
		if (realm == null)
		{
			if (realms == null || userdata == null)
				return null;
			realm = userdata.toString();
			Object val = realms.get(realm);
			if (val == null)
				return null;
			robj = (Map)val;
			userdata = robj.get("file").toString();
			crypted = Boolean.getBoolean(robj.get("crypted").toString());
		}
		else if (realms != null)
		{
			Object val = realms.get(realm);
			if (val != null)
				robj = (Map)val;
		}

		Stream src = null;
		Object users[] = null;
		long time = 0;
		if (userdata instanceof Stream)
			src = (Stream)userdata;
		else if (Stream.isStreamable(userdata, Stream.INPUT))
			src = new Stream(userdata);
		else if (userdata instanceof Object[])
			users = (Object[])userdata;
		else if (userdata instanceof List)
			users = ((List)userdata).toArray();
		else if (userdata instanceof Scriptable)
			users = Interpreter.getArray((Scriptable)userdata);
		else
			return robj;
		if (src != null)
		{
			time = src.lastModified();
			if (robj != null)
			{
				long robjtime = ((Number)(robj.get("time"))).longValue();
				if (robjtime > 0 && robjtime >= time)
					return robj;
			}
			users = Interpreter.getArray(src.readAll());
		}

		if (users == null)
			return null;
		List lst = new ArrayList();
		for (int i = 0; i < users.length; i++)
		{
			Object val = Interpreter.getObject(users[i]);
			if (val instanceof Map)
			{
				Map map = (Map)val;
				if (map.get("user") != null && map.get("pass") != null)
					lst.add(map);
			}
			else if (val instanceof Scriptable)
			{
				Scriptable data = (Scriptable)val;
				Map map = new HashMap();
				val = data.get("user", data);
				if (val == null)
					continue;
				map.put("user", val.toString());
				val = data.get("pass", data);
				if (val == null)
					continue;
				map.put("pass", val.toString());
				lst.add(map);
			}
			else if (val instanceof Object[])
			{
				Object data[] = (Object[])val;
				if (data.length > 1 && data[0] != null && data[1] != null)
				{
					Map map = new HashMap();
					map.put("user", data[0].toString());
					map.put("pass", data[1].toString());
					lst.add(map);
				}
			}
			else if (val instanceof List)
			{
				List data = (List)val;
				if (data.size() < 2)
					continue;
				Map map = new HashMap();
				val = data.get(0);
				if (val == null)
					continue;
				map.put("user", val.toString());
				val = data.get(1);
				if (val == null)
					continue;
				map.put("pass", val.toString());
				lst.add(map);
			}
			else if (val != null)
			{
				String txt = val.toString().trim();
				String parts[] = txt.split(":");
				if (parts.length < 2)
				{
					parts = txt.split(",");
					if (parts.length < 2)
						continue;
				}
				Map map = new HashMap();
				map.put("user", parts[0]);
				map.put("pass", parts[1]);
				lst.add(map);
			}
		}

		if (lst.size() == 0)
			return null;
		else if (robj == null)
		{
			if (realms == null)
				realms = new HashMap();
			robj = new HashMap();
			realms.put(realm, robj);
		}

		robj.put("file", (src == null) ? "" : src.getSource().toString());
		robj.put("realm", realm);
		robj.put("crypted", crypted?"true":"false");
		robj.put("time", new Long(time));
		robj.put("users", lst);
		return robj;
	}

	private void sendStatus(HttpServletRequest req, HttpServletResponse res, String user, int code, String msg)
	{
		sendStatus(req, res, user, code, msg, "Authentication required", null);
	}

	private void sendStatus(HttpServletRequest req, HttpServletResponse res, String user, int code, String msg, String realm)
	{
		sendStatus(req, res, user, code, msg, "Authentication required", null);
	}

	private void sendStatus(HttpServletRequest req, HttpServletResponse res, String user, int code, String msg, String realm, String redirect)
	{
		try
		{
			if (redirect != null && !redirect.equals("") && !redirect.equals("null") && !redirect.equals("undefined"))
				res.sendRedirect(redirect);
			else
			{
				res.setStatus(code);
				if (code == 401)
					res.setHeader("WWW-Authenticate", "Basic realm=\""+realm+"\"");
				if (code >= 400)
				{
					res.getWriter().println(code+": "+msg+"<p>\n");
					res.flushBuffer();
				}
			}
			if (userlog != null)
			{
				String path;
				Date dt = new Date();
				if (userlog[1] == null)
					path = userlog[0];
				else
				{
					path = userlog[0]
							+(dt.getYear()+1900)
							+((dt.getMonth()+101)+"").substring(1)
							+((dt.getDate()+100)+"").substring(1)
							+userlog[1];
				}
				FileOutputStream f = new FileOutputStream(path, true);
				String q = req.getQueryString();
				if (q == null)
					q = "";
				else if (!q.equals(""))
					q = "?"+q;
				String txt = (dt.getYear()+1900)+"-"
								+((dt.getMonth()+101)+"").substring(1)+"-"
								+((dt.getDate()+100)+"").substring(1)+" "
								+((dt.getHours()+100)+"").substring(1)+":"
								+((dt.getMinutes()+100)+"").substring(1)+":"
								+((dt.getSeconds()+100)+"").substring(1)+","
								+req.getRemoteAddr()+","+user+","+code+","
								+req.getRequestURI()+q+","+msg+"\n";
				f.write(txt.getBytes());
				f.close();
			}
		}
		catch (Exception e) { log(e.toString()); }
	}

	public String getInitParameter(String name)
	{
		if (name == null || name.equals(""))
			return null;
		else if (parameters != null)
		{
			Object val = parameters.get(name);
			return (val == null) ? null : val.toString();
		}
		parameters = new HashMap();
		java.util.Enumeration namelst = super.getInitParameterNames();
		if (namelst == null)
		{
			String path = getServletContext().getRealPath("");
			Map map = loadParameters(path);
			Object node = getMap(map, "/web-app/servlet/init-param");
			List lst;
			if (node instanceof List)
				lst = (List)node;
			else
			{
				lst = new ArrayList();
				lst.add(node);
			}
			int nb = lst.size();
			for (int i = 0; i < nb; i++)
			{
				node = lst.get(0);
				if (node == null || !(node instanceof Map))
					continue;
				map = (Map)node;
				Object var = map.get("param-name");
				Object val = map.get("param-value");
				if (var != null && !var.equals("") && val != null)
					parameters.put(var.toString(), val.toString());
			}
		}
		else
		{
			List keys = new ArrayList();
			while (namelst.hasMoreElements())
				keys.add(namelst.nextElement().toString());
			int nb = keys.size();
			for (int i = 0; i < nb; i++)
			{
				String var = keys.get(i).toString();
				parameters.put(var, super.getInitParameter(var));
			}
		}
		return getInitParameter(name);
	}

	public static Map loadParameters(String path)
	{
		Map map = new HashMap();
		char sep = File.separatorChar;
		while (!path.equals(""))
		{
			if (path.charAt(path.length()-1) == sep)
				path = path.substring(0, path.length()-1);
			File f = new File(path+sep+"WEB-INF");
			if (f.exists() && f.isDirectory())
			{
				f = new File(path+sep+"WEB-INF/web.xml");
				if (!f.exists() || f.isDirectory())
					continue; // or break;
				try
				{
					InputStream in = new FileInputStream(f);
					int n = (int)(f.length());
					byte b[] = new byte[n];
					in.read(b);
					in.close();
					String src = new String(b);
					List tags = new ArrayList();
					boolean intag = false, end = false;
					for (int at = 0, p = 0; !end; at = p + 1)
					{
						String search = intag ? (src.substring(at, at+3).equals("!--") ? "-->" : ">") : "<";
						p = src.indexOf(search, at);
						if (p < 0)
						{
							p = src.length();
							end = true;
						}
						tags.add(src.substring(at-(intag?1:0), p));
						intag = !intag;
					}
					parseTagList(tags, map, 0, null);
					break;
				}
				catch (Exception e) { new RuntimeException(e.toString()); }
			}
			int pos = path.lastIndexOf(sep);
			if (pos < 0)
				break;
			path = path.substring(0, pos);
		}
		return map;
	}

	public static Object getMap(Map map, String path)
	{
		if (map == null || path == null || path.equals(""))
			return null;
		else if (path.charAt(0) == '/')
			path = path.substring(1);
		String lst[] = path.split("/");
		Object val = null;
		for (int i = 0; i < lst.length && map != null; i++)
		{
			String name = lst[i];
			val = map.get(name);
			if (val == null)
				return null;
			else if (val instanceof Map)
				map = (Map)val;
			else if (i+1 < lst.length)
			{
				map = null;
				while (val instanceof List)
				{
					List sub = (List)val;
					if (sub.size() > 0)
					{
						val = sub.get(0);
						if (val instanceof Map)
							map = (Map)val;
					}
					else
						val = null;
				}
				if (map == null)
					return null;
			}
		}
		return val;
	}

	private static int parseTagList(List tags, Map dst, int start, String parent)
	{
		int n = tags.size();
		for (int i = start; i < n; i++)
		{
			Object val = tags.get(i);
			if (val == null || val.equals(""))
				continue;
			String txt = val.toString();
			if (txt.charAt(0) != '<')
			{
				txt = txt.trim();
				if (parent != null && !txt.equals(""))
					dst.put("text", txt);
				continue;
			}
			char first = txt.charAt(1);
			switch (first)
			{
			case '?':
			case '!':
				continue;
			case '/':
				if (txt.substring(2).toLowerCase().equals(parent))
					return i;
				break;
			default:
				if (first < 'A' || first > 'z' || (first > 'Z' && first < 'a'))
					continue;
				List lst = parseTagProperties(txt.substring(1));
				String name = lst.get(0).toString().toLowerCase();
				Map map = new HashMap();
				String last = null;
				String param = null;
				int x = lst.size();
				for (int e = 1; e < x; e++)
				{
					val = lst.get(e);
					if (val.equals("=") && last != null && !last.equals(""))
						param = last;
					else if (param != null)
					{
						map.put(param, val);
						param = null;
					}
					last = val.toString();
				}
				if (param != null)
					map.put(param, "");
// check if last == "/" so we don't get down to sub nodes
				i = parseTagList(tags, map, i+1, name);
				Object sub = map;
				Object keys[] = map.keySet().toArray();
				if (keys.length == 1 && keys[0].equals("text"))
					sub = map.get("text");
				val = dst.get(name);
				if (val == null)
					dst.put(name, sub);
				else if (val instanceof List)
					((List)val).add(sub);
				else
				{
					lst = new ArrayList();
					lst.add(val);
					lst.add(sub);
					dst.put(name, lst);
				}
			}
		}

		return n;
	}

	private static List parseTagProperties(String txt)
	{
		List lst = new ArrayList();
		int len = txt.length();
		int start = -1;
		char instr = 0;

		for (int i = 0; i < len; i++)
		{
			char c = txt.charAt(i);
			if (instr != 0)
			{
				if (c == instr)
				{
					lst.add(txt.substring(start, i));
					instr = 0;
					start = -1;
				}
			}
			else if (c <= ' ' || c == '=')
			{
				if (start >= 0)
				{
					lst.add(txt.substring(start, i));
					start = -1;
				}
				if (c == '=')
					lst.add("=");
			}
			else if (start < 0)
			{
				start = i;
				if (c == '\"' || c == '\'')
				{
					start++;
					instr = c;
				}
			}
		}

		if (start >= 0)
			lst.add(txt.substring(start, len));
		if (lst.size() == 0)
			lst.add("?");
		else if (lst.get(0).equals(""))
			lst.set(0, "?");
		return lst;
	}
}


class ScriptThread extends Thread
{
	private Interpreter js = null;
	private String script = null;
	private Servlet servlet = null;
	private Map application = null;

	public ScriptThread(Object src, Servlet srv, Map app, String name)
	{
		setDaemon(false);
		if (name != null && !name.equals(""))
			setName(name);
		servlet = srv;
		application = app;
		if (src == null)
			script = "";
		else if (src instanceof File)
			script = "load("+Interpreter.escapeString(src.toString())+");";
		else if (src instanceof FunctionObject)
		{
			FunctionObject fn = (FunctionObject)src;
			script = fn.getFunctionName()+"();\n"+fn.toString()+"\n";
		}
		else
			script = src.toString();
	}

	public String toString()
	{
		return "[ScriptThread "+getName()+"]";
	}

	public void run()
	{
		js = new Interpreter(servlet);
		js.defineProperty(Servlet.APPLICATION, Interpreter.newObject(application, js), 0);
		js.eval(script, this.toString());
      Interpreter.terminate(js);
      if (servlet != null)
         servlet.killScript(getName());
	}

	public Interpreter getGlobal()
	{
		return js;
	}
}

class HttpInputStream extends InputStream implements javax.activation.DataSource
{
	private String name;
	private InputStream src;
	private String mimetype;
	private int length, got=0;
	private Interpreter script;
	private boolean streamCalled=false;

	public HttpInputStream(String txt, String type, InputStream in, int nb, Interpreter js)
	{
		name = txt;
		src = in;
		mimetype = type;
		length = nb;
		script = js;
	}

	public float getCompleted() { return got / (float)length; }

	public void load()
	{
		try
		{
			javax.mail.internet.MimeMultipart m = new javax.mail.internet.MimeMultipart(this);
			int nb = m.getCount();
			for (int i = 0; i < nb; i++)
			{
				javax.mail.BodyPart part = m.getBodyPart(i);
				java.util.Enumeration lst = part.getAllHeaders();
				String varname = null;
				String filename = null;
				while(lst.hasMoreElements())
				{
					Object elem = lst.nextElement();
					if (elem instanceof javax.mail.Header)
					{
						javax.mail.Header h = (javax.mail.Header)elem;
						if (varname == null)
							varname = extractData("name", h.getValue());
						if (filename == null)
							filename = extractData("filename", h.getValue());
					}
				}
				if (varname != null)
				{
					Object val = part.getContent();
					if (filename != null)
						val = saveFile(filename, val);
					script.getServlet().setValue(script, varname, val);
				}
			}
		}
		catch (Exception e) { throw new RuntimeException(e.toString());}
	}

	private String extractData(String var, String txt)
	{
		if (txt == null || txt.equals(""))
			return null;
		int p = txt.indexOf(var+"=");
		if (p < 0)
			return null;
		txt = txt.substring(p+var.length()+1);
		if (txt.charAt(0) == '"')
		{
			txt = txt.substring(1);
			p = txt.indexOf('"');
			if (p >= 0)
				txt = txt.substring(0,p);
		}
		else
		{
			p = txt.indexOf(';');
			if (p >= 0)
				txt = txt.substring(0,p);
		}

		p = txt.lastIndexOf('/');
		if (p >= 0)
			txt = txt.substring(p+1);
		p = txt.lastIndexOf('\\');
		if (p >= 0)
			txt = txt.substring(p+1);
		return txt;
	}

	private Object saveFile(String filename, Object data)
	{
		try
		{
			String upload;
			Object app = script.get(Servlet.APPLICATION, script);
			Object val = ((Scriptable)app).get("upload", script);
			if ((val instanceof String) || (val instanceof File))
				upload = val.toString();
			else if (val instanceof Stream)
				upload = ((Stream)val).getSource().toString();
			else
			{
				HttpServletRequest req = (HttpServletRequest)(Interpreter.getObject(script.get("request", script)));
				upload = req.getRequestURI();
				int p = upload.lastIndexOf('/');
				int p2 = upload.lastIndexOf('\\');
				if (p < 0 || p2 > p)
					p = p2;
				if (p >= 0)
					upload = upload.substring(0,p+1);
				else
					upload += File.separator;
				upload += "upload";
				String base = req.getContextPath();
				if (!base.equals("") && upload.startsWith(base))
					upload = upload.substring(base.length());
				upload = script.getServlet().getServletContext().getRealPath(upload);
			}
			char last = upload.charAt(upload.length()-1);
			if (last != '/' && last != '\\')
				upload += File.separator;
			File f = new File(upload+filename);
			FileOutputStream out = new FileOutputStream(f);
			if (data instanceof String)
				out.write(data.toString().getBytes());
			else if (data instanceof InputStream)
			{
				InputStream src = (InputStream)data;
				byte b[] = new byte[1024];
				int n;
				while ( (n = src.read(b)) >= 0 )
					out.write(b, 0, n);
			}
			else if (data instanceof Object[])
			{
				Object [] lst = (Object[])data;
				for (int i = 0; i < lst.length; i++)
					out.write(lst[i].toString().getBytes());
			}
			out.close();
			return Interpreter.newObject(f, script);
		}
		catch (Exception e) { return e.toString(); }
	}

/************* DataSource interface ************************/
	public String getContentType() { return mimetype; }

   public InputStream getInputStream() throws IOException
   {
   	if (streamCalled)
   		throw new IOException("This method has already been called");
   	streamCalled = true;
   	return this;
   }

   public OutputStream getOutputStream() throws IOException
   {
   	throw new IOException("This object does not support OutputStream");
   }

   public String getName() { return name; }

/****************** InputStream extension *************************/

	public int available() throws IOException
	{
		int n = (got >= length) ? 0 : (length - got);
		return n;
	}

	public void close() throws IOException
	{
		src.close();
	}

	public void mark(int readlimit) { src.mark(readlimit); }

	public boolean markSupported() { return src.markSupported(); }

	public int read() throws IOException
	{
		byte b[] = new byte[1];
		int nb = read(b, 0, 1);
		if (nb < 1)
			return -1;
		int n = (int)(b[0]);
		if (n < 0)
			n += 128;
		got++;
		return n;
	}

	public int read(byte[] b) throws IOException
	{
		return read(b, 0, b.length);
	}

	public int read(byte[] b, int off, int len) throws IOException
	{
		int n = src.read(b, off, len);
		if (n > 0)
			got += n;
		return n;
	}

	public void reset() throws IOException { src.reset(); }

	public long skip(long n) throws IOException
	{
		n = src.skip(n);
		if (n > 0)
			got += n;
		return n;
	}
}


