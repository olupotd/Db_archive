/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package executor;

import java.awt.Rectangle;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.DefaultListModel;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;

public class Setup {

	private static Connection con;
	private static Statement st;
	public String destServerUser;
	public String destServerPass;
	public String option;
	// table_name, Retention_rate
	Map<String, String> retRate = new HashMap<String, String>();
	private Config config;
	public String loggedServer;
	public String URL;
	int i = 0;
	File f;
	PrintWriter pr;
	public String loginPass, loginUser;
	public boolean accepted;
	public Date start_date;
	@SuppressWarnings("rawtypes")
	public JList query_show;

	public Setup() {
	}

	public boolean saveConfig(List<String> newConfig) {
		try {
			Properties newProps = new Properties();
			profile = new File("config/config.xml");
			for (String prop : newConfig) {
				String params[] = prop.split(";");
				String property = "jdbc:sybase:Tds:" + params[0] + ":"
						+ params[1] + "/" + params[2];
				newProps.setProperty("server" + i, property);
			}
			fileOut = new FileOutputStream(profile);
			newProps.storeToXML(fileOut, "Server Configs");
			return true;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	@SuppressWarnings("unused")
	private void log(String data) {
		try {
			f = new File("failed_tables.log");
			// check for the file size and if it exceeds 500mb
			if (f.length() > 3055 * 1024) {
				f.delete();
			}
			if (!f.exists()) {
				f.createNewFile();
			}
			pr = new PrintWriter(new FileWriter(f, true));
			pr.println(data);
			pr.flush();
			pr.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public Connection dest;
	Date stop_date;

	@SuppressWarnings({ "deprecation", "unused" })
	private Date dateDiff(Date finish_date, Date start_date) {
		Date diff = new Date();
		diff.setHours(finish_date.getHours() - start_date.getHours());
		diff.setMinutes(finish_date.getMinutes() - start_date.getMinutes());
		diff.setSeconds(finish_date.getSeconds() - start_date.getSeconds());
		return diff;
	}

	JLabel durations;
	String start_time;
	HashMap<String, String> completedList = new HashMap<>();

	public void loadDatabases() {
		try {
			st = con.createStatement();
			ResultSet dbs = st.executeQuery("sp_helpdb");
			while (dbs.next()) {
				config.setDbList(dbs.getString("name"));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public boolean isConnected() {
		try {
			return !con.isClosed();
		} catch (SQLException e) {
			JOptionPane.showMessageDialog(null, e.getLocalizedMessage());
		}
		return false;
	}

	// private static String username, password;
	public String RemoteuserN, Remotepass;

	public boolean getConnected(String url) {
		try {
			Class.forName("com.sybase.jdbc3.jdbc.SybDriver").newInstance();
			con = DriverManager.getConnection("jdbc:sybase:Tds:" + url,
					loginUser, loginPass);
			if (!con.isClosed()) {
				return true;
			}
		} catch (Exception e) {
			JOptionPane.showMessageDialog(null, e.getLocalizedMessage());
		}
		return false;

	}

	public Vector<String> loadTables(String string) {
		Vector<String> tables = new Vector<>();
		try {
			// st = getConnected(loggedServer);
			st = con.createStatement();
			ResultSet result = st
					.executeQuery("select name from sysobjects where type = 'U' order by 1");
			// /////// Just for curiosity
			while (result.next()) {
				tables.add(result.getString("name"));
			}
		} catch (SQLException e) {
		}
		return tables;
	}

	ResultSet rset = null;
	String sql = null;

	@SuppressWarnings("unused")
	private List<String> load_retention(List<String> tables) {
		List<String> temp_holder = new ArrayList<>();
		try {
			for (String table : tables) {
				sql = "SELECT DATEADD(day,"
						+ (-1 * Integer.parseInt(table.split(";")[1]))
						+ ", last_to_dt) AS ret_dt FROM ov_control";
				ResultSet set = con.createStatement().executeQuery(sql);
				while (set.next()) {
					temp_holder.add(table.split(";")[0] + ";"
							+ set.getObject("ret_dt").toString().split(" ")[0]);
				}
				set.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return temp_holder;
	}

	List<String> selectedTabs = new ArrayList<>();
	Properties properties;
	File file, profile;
	FileOutputStream fileOut;

	public List<String> loadConfigs() {
		Properties savedConfigs = new Properties();
		FileInputStream inputS;
		List<String> conf = new ArrayList<>();
		File prop = new File("config/tables.xml");
		try {
			if (prop.exists()) {
				inputS = new FileInputStream(prop);
				savedConfigs.loadFromXML(inputS);
				for (String key : savedConfigs.stringPropertyNames()) {
					conf.add(key);
				}
			} else {
				prop.createNewFile();
			}
		} catch (IOException ex) {
			Logger.getLogger(Setup.class.getName()).log(Level.SEVERE, null, ex);
		} finally {
		}
		return conf;
	}

	private List<String> loadProfiles() {
		List<String> lists = new ArrayList<>();
		Properties profiles = new Properties();
		File tables = new File("config/tables.xml");
		try {
			if (!tables.exists()) {
				tables.createNewFile();
			} else {
				FileInputStream in = new FileInputStream(tables);
				profiles.loadFromXML(in);
				// Fetch all the config params as well
				for (String key : profiles.stringPropertyNames()) {
					if (!key.equals("license")) {
						lists.add(key + ";" + profiles.getProperty(key));
					}
				}
			}
		} catch (IOException ex) {
			Logger.getLogger(Setup.class.getName()).log(Level.SEVERE, null, ex);
		}
		return lists;
	}

	Properties props;
	File object;

	public boolean writeProfile(String configName, Tables tables) {
		Properties props = new Properties();
		profile = new File("config/tables.xml");
		if (!new File("config/objects").exists()) {
			new File("config/objects").mkdirs();
		}
		File objectFile = new File("config/objects/" + configName);
		// Load the existing properties from the file
		for (String pro : loadProfiles()) {
			String[] conf = pro.split(";");
			props.setProperty(conf[0], conf[1]);
		}
		props.setProperty(configName, objectFile.getName());
		// Save the config to an objectFile file
		try {
			// Save the config created by user.
			fileOut = new FileOutputStream(profile);
			props.storeToXML(fileOut, "Server Configs");
			fileOut.close();
			// Save the Object File
			try (FileOutputStream fos = new FileOutputStream(objectFile)) {
				ObjectOutputStream oos = new ObjectOutputStream(fos);
				oos.writeObject(tables);
				oos.flush();
				oos.close();
			}
			return true;
		} catch (IOException ex) {
			return false;
		}
	}

	Rectangle progRect;
	int count = 1;

	public Config getConfig() {
		return this.config;
	}

	FileInputStream in;

	public boolean isAdmin() {
		List<String> users = new ArrayList<>();
		boolean admin = false;
		try {
			rset = con.createStatement().executeQuery("sp_displayroles");
			while (rset.next()) {
				users.add(rset.getObject(1).toString());
			}
			for (String user : users) {
				if ("sa_role".equals(user)) {
					admin = true;
					break;
				}
			}
		} catch (SQLException e) {
		}
		return admin;
	}

	public boolean checkConfigExists(String config) {
		if (new File("config/objects/" + config).exists()) {
			return true;
		}
		return false;
	}

	@SuppressWarnings("resource")
	public Tables loadSelectedTables(String slctdConfig) {
		FileInputStream fin;
		Tables tabs;
		try {
			fin = new FileInputStream("config/objects/" + slctdConfig);
			ObjectInputStream ois = new ObjectInputStream(fin);
			tabs = (Tables) ois.readObject();
			return tabs;
		} catch (FileNotFoundException ex) {
			Logger.getLogger(Setup.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IOException | ClassNotFoundException ex) {
			Logger.getLogger(Setup.class.getName()).log(Level.SEVERE, null, ex);
		}
		return null;
	}

	@SuppressWarnings("rawtypes")
	DefaultListModel lm2 = new DefaultListModel();
	// DefaultListModel quer = new DefaultListModel();
	@SuppressWarnings("unused")
	private String sourceServer, destServer;

	public void setSourceServer(String string) {
		this.sourceServer = string;
	}

	public void setDestinationServer(String string) {
		this.destServer = string;
	}

	public boolean removeConfig(String slc) {
		if (checkConfigExists(slc)) {
			properties = new Properties();
			try {
				FileInputStream in = new FileInputStream(new File(
						"config/tables.xml"));
				properties.loadFromXML(in);
				properties.remove(slc);
				FileOutputStream fos = new FileOutputStream(new File(
						"config/tables.xml"));
				properties.storeToXML(fos, "Database configs");
				fos.close();
				in.close();
				return true;
			} catch (IOException ex) {
				Logger.getLogger(Setup.class.getName()).log(Level.SEVERE, null,
						ex);
			}
		}
		return false;
	}

	File table_file;
	SimpleDateFormat today = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
}
