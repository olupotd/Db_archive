package ui;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import javax.swing.JOptionPane;

import executor.License;
import executor.Server;
import executor.Setup;

public class Main {

	String[] user_credentials = new String[8];
	private Server server;
	static Connection loginConnection, remoteConnection;
	ResultSet set;
	protected boolean accepted;
	private String sql = null;

	public Server getServer() {
		return server;
	}

	public void setServer(Server server) {
		this.server = server;
	}

	public List<String> load_retention(List<String> tables) {
		List<String> temp_holder = new ArrayList<>();
		try {
			for (String table : tables) {
				sql = "SELECT DATEADD(day,"
						+ (-1 * Integer.parseInt(table.split(";")[1]))
						+ ", last_to_dt) AS ret_dt FROM ov_control";
				ResultSet set = loginConnection.createStatement().executeQuery(
						sql);
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

	public String[] getUser_credentials() {
		return user_credentials;
	}

	private Cipher ecipher, dcipher;
	static final String passPhrase = "BbcWorldService";

	public Main() {
		// 8-bytes Salt
		byte[] salt = { (byte) 0xA9, (byte) 0x9B, (byte) 0xC8, (byte) 0x32,
				(byte) 0x56, (byte) 0x34, (byte) 0xE3, (byte) 0x03 };
		// Iteration count
		int iterationCount = 19;
		try {
			KeySpec keySpec = new PBEKeySpec(passPhrase.toCharArray(), salt,
					iterationCount);
			SecretKey key = SecretKeyFactory.getInstance("PBEWithMD5AndDES")
					.generateSecret(keySpec);
			ecipher = Cipher.getInstance(key.getAlgorithm());
			dcipher = Cipher.getInstance(key.getAlgorithm());
			// Prepare the parameters to the cipthers
			AlgorithmParameterSpec paramSpec = new PBEParameterSpec(salt,
					iterationCount);
			ecipher.init(Cipher.ENCRYPT_MODE, key, paramSpec);
			dcipher.init(Cipher.DECRYPT_MODE, key, paramSpec);
		} catch (InvalidAlgorithmParameterException e) {
			System.out.println("EXCEPTION: InvalidAlgorithmParameterException");
		} catch (InvalidKeySpecException e) {
			System.out.println("EXCEPTION: InvalidKeySpecException");
		} catch (NoSuchPaddingException e) {
			System.out.println("EXCEPTION: NoSuchPaddingException");
		} catch (NoSuchAlgorithmException e) {
			System.out.println("EXCEPTION: NoSuchAlgorithmException");
		} catch (InvalidKeyException e) {
			System.out.println("EXCEPTION: InvalidKeyException");
		}
	}

	@SuppressWarnings({ "unused" })
	public String decrypt(String license_code) {
		try {
			// Decode base64 to get bytes
			byte[] dec = new sun.misc.BASE64Decoder()
					.decodeBuffer(license_code);
			// Decrypt
			byte[] utf8 = dcipher.doFinal(dec);
			// Decode using utf-8
			String valid = new String(utf8, "UTF8");
			// validate(valid)
			return new String(utf8, "UTF8");
		} catch (BadPaddingException e) {
		} catch (IllegalBlockSizeException e) {
		} catch (UnsupportedEncodingException e) {
		} catch (IOException e) {
		}
		return null;
	}

	public boolean isAdmin() {
		List<String> users = new ArrayList<>();
		boolean admin = false;
		try {
			set = loginConnection.createStatement().executeQuery(
					"sp_displayroles");
			while (set.next()) {
				users.add(set.getObject(1).toString());
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

	public void closeConnection() {
		try {
			if (!loginConnection.isClosed()) {
				loginConnection.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void log_summary(List<String> doneTables, String start_time,
			String end_time) {
		PrintWriter pr = null;
		;
		try {
			pr = new PrintWriter(new FileWriter(new File(
					new SimpleDateFormat().format(new Date()) + ".log"), true));
			pr.println("Start Time:" + start_time);
			pr.println("Table Count:" + doneTables.size());
			for (String tables : doneTables) {
				pr.println(tables);
			}
			pr.println("Stop Time:" + end_time);
			pr.flush();
		} catch (IOException ex) {
			Logger.getLogger(Setup.class.getName()).log(Level.SEVERE, null, ex);
		} finally {
			pr.close();
		}
	}

	public String date_diff(Date start_dt, Date finish_dt) {
		long diff = finish_dt.getTime() - start_dt.getTime();
		long diffSeconds = diff / 1000 % 60;
		long diffMinutes = diff / (60 * 1000) % 60;
		long diffHours = diff / (60 * 60 * 1000) % 24;
		long diffDays = diff / (24 * 60 * 60 * 1000);
		return "Days (" + diffDays + ") Hours (" + diffHours + ") minutes ("
				+ diffMinutes + ") Seconds (" + diffSeconds + ")";
	}

	public void recordTime(List<String> tables, int tHREAD_NUM,
			String date_diff, Date startTime, Date stopTime) {
		// TODO Auto-generated method stub
		File f = new File(
				"config/App_logs/"
						+ new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
								.format(new Date()) + ".log");
		try {
			if (f.exists()) {
				f.createNewFile();
			}
			PrintWriter pr = new PrintWriter(new FileWriter(f));
			pr.println("================================================================");
			pr.println("Operation Date:"
					+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
							.format(new Date()));
			pr.println("Started Process at: "
					+ new SimpleDateFormat("HH:mm:ss").format(startTime));
			pr.println("Thread Number: " + tHREAD_NUM);
			pr.println("Number of Tables: " + tables.size());
			pr.println("Tables processed");
			pr.println("________________________________________________________________");
			for (String table : tables) {
				pr.println("\tTable Name:" + table.split(";")[0]
						+ " Retention: " + table.split(";")[1]);
			}
			pr.println("________________________________________________________________");
			pr.println("Duration: " + date_diff);
			pr.println("End time: "
					+ new SimpleDateFormat("HH:mm:ss").format(stopTime));
			pr.println("================================================================");
			pr.flush();
			pr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String bank_name() {
		String sql = "SELECT name_1 FROM ad_gb_bank";
		ResultSet set;
		Object date = null;
		try {
			set = loginConnection.createStatement().executeQuery(sql);
			while (set.next()) {
				date = set.getObject("name_1");
			}
			return date.toString().split(" ")[0];
		} catch (SQLException e) {
		}
		return null;
	}

	// select name_1 from ad_gb_bank
	public int get_Ov_date() {
		String sql = "Select datediff(day,(dateadd(day,1,last_to_dt)), '"
				+ decrypt(getLicense().getExpiry_date())
				+ "') as ret_dt from ov_control";
		ResultSet set;
		int date = 0;
		try {
			set = loginConnection.createStatement().executeQuery(sql);
			while (set.next()) {
				date = set.getInt("ret_dt");
			}
		} catch (SQLException e) {
		}
		return date;
	}

	@SuppressWarnings("resource")
	public License getLicense() {
		try {
			if (!new File("License.dat").exists()) {
				JOptionPane
						.showMessageDialog(null,
								"License Not Found. Add a License file to the base Directory to proceed.");
				System.exit(0);
			}
			FileInputStream fis = new FileInputStream("License.dat");
			ObjectInputStream ois = new ObjectInputStream(fis);
			return (License) ois.readObject();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			JOptionPane.showMessageDialog(null, "License not found.");
			e.printStackTrace();
		}
		return null;
	}

	public void validateLicense() {
		// Centenary Rural Development Bank Ltd
		License li = getLicense();
		if (li.getApp_name().equals("Dynamic_Archival")
				&& li.getBank_name().equals(
						"Centenary Rural Development Bank Ltd")) {
			int diff = get_Ov_date();
			System.out.println(diff);
			if (diff < 30 && diff >= 0) {
				// License due to expire
				JOptionPane.showMessageDialog(null, "License expires in "
						+ diff + " day(s)");
			} else if (diff < 0) {
				JOptionPane.showMessageDialog(null,
						"License has expired. Contact Neptune for a renewal");
				System.exit(0);
			}
		} else {
			JOptionPane.showMessageDialog(null,
					"The License is not valid for this application.");
			System.exit(0);
		}

	}

	public Vector<String> loadTables(String string) {
		Vector<String> tables = new Vector<>();
		try {
			// st = getConnected(loggedServer);
			Statement st = loginConnection.createStatement();
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

	public boolean isConnected() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		Class.forName("com.sybase.jdbc3.jdbc.SybDriver").newInstance();
		try {
			loginConnection = DriverManager.getConnection("jdbc:sybase:Tds:"
					+ server.local_url, server.local_user, server.local_pass);
			return !loginConnection.isClosed();
		} catch (SQLException e) {
			JOptionPane.showMessageDialog(null, e.getLocalizedMessage());
		}
		return false;
	}

	@SuppressWarnings("deprecation")
	public String getDuration(Date finish_dt, Date start_dt) {
		Date diff = new Date();
		diff.setHours(finish_dt.getHours() - start_dt.getHours());
		diff.setMinutes(finish_dt.getMinutes() - start_dt.getMinutes());
		diff.setSeconds(finish_dt.getSeconds() - start_dt.getSeconds());
		return new SimpleDateFormat("HH:mm:ss").format(diff);
	}
}
