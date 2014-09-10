package cons;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.swing.JLabel;

public class Single_Inserter implements Runnable {

	// ip:port/db;user;pass|ip:port/db;user;pass
	private String table, retention, option, sql, query;
	private Connection source_con, dest_con;
	private String source_url, dest_url;
	private Properties source_props, dest_props;
	ResultSet rSet = null;
	ResultSetMetaData rMeta;
	List<String> columns;
	int rowcounts;
	private Statement stmt;
	int i, total_sub_tables;
	private boolean flag;
	private int total_rows;
	private JLabel status;
	Thread t;

	public Single_Inserter(String tab_props, String option, String urls,
			String logins, JLabel status) throws InterruptedException {
		try {
			columns = new ArrayList<>();
			source_props = new Properties();
			dest_props = new Properties();
			this.table = tab_props.split(";")[0];
			this.retention = tab_props.split(";")[1];
			this.status = status;
			this.option = option;
			// Split the Source and Dest URLs
			this.source_url = urls.split(";")[0];
			this.dest_url = urls.split(";")[1];
			// System.out.println("Started Table " + table);
			// Set the Logins for the user
			source_props.put("user", logins.split(";")[0]);
			source_props.put("password", logins.split(";")[1]);
			dest_props.put("user", logins.split(";")[2]);
			dest_props.put("password", logins.split(";")[3]);
			Class.forName("com.sybase.jdbc4.jdbc.SybDriver").newInstance();
			source_con = DriverManager.getConnection("jdbc:sybase:Tds:"
					+ source_url, source_props);
			if (!source_con.isClosed())
				stmt = source_con.createStatement();
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			// e.printStackTrace();
		} catch (SQLException e) {
			// e.printStackTrace();
			log(e.getLocalizedMessage());
		}
	}

	@SuppressWarnings("unused")
	private int get_total_spaceused(String tabl) {
		int total = 0;
		try {
			ResultSet results = stmt.executeQuery("sp_spaceused " + tabl);
			while (results.next())
				total = results.getInt("rowtotal");
			results.close();
			this.record_total = total;
		} catch (SQLException e) {
			log(e.getLocalizedMessage());
		}
		return total;
	}

	private int get_spaceused() {
		int total = 0;
		try {
			ResultSet results = stmt.executeQuery("sp_spaceused " + table);
			while (results.next())
				total = results.getInt("rowtotal");
			results.close();
			this.record_total = total;
		} catch (SQLException e) {
			log(e.getLocalizedMessage());
		}
		return total;
	}

	private void run_purge() {
		try {
			status.setText("Started Purge on " + table);
			Connection sourceCon = DriverManager.getConnection(
					"jdbc:sybase:Tds:" + source_url, source_props);
			if (!sourceCon.isClosed()) {
				Statement sourceStmt = sourceCon.createStatement();
				sourceCon.setAutoCommit(true);
				if (option.contains("archive"))
					rowcounts = compute_batches(total_rows);
				else {
					sql = "SELECT COUNT(create_dt) AS result FROM " + table
							+ " (INDEX da_create_dt) WHERE create_dt <= '"
							+ retention + "'";
					rSet = sourceStmt.executeQuery(sql);
					while (rSet.next()) {
						rowcounts = compute_batches(rSet.getInt("result")); // total_rows
					}
				}
				int count = 0;
				while (count <= rowcounts) {
					sql = "SET ROWCOUNT 50000 DELETE " + table + " FROM "
							+ table
							+ " (INDEX da_create_dt) WHERE create_dt <= '"
							+ retention + "'";
					sourceStmt.executeUpdate(sql);
					status.setText("Dumping tran with no_log");
					sourceStmt.executeUpdate("dump tran "
							+ source_url.split("/")[1] + " with no_log");
					status.setText("dumping tran on tempdb");
					sourceStmt.executeUpdate("dump tran tempdb with no_log");
					count++;
					// status.setText("Purged: " + count);
				}
				status.setText("Updating stats for " + table);
				sourceStmt.executeUpdate("update index statistics " + table
						+ " da_create_dt with sampling=20 percent");
				status.setText("Purged: " + table);
				rSet.close();
				sourceStmt.close();
				sourceCon.close();
			}
			source_con.close();
		} catch (SQLException e) {
			log(e.getLocalizedMessage());
		}
	}

	@SuppressWarnings("unused")
	private int count_rows() {
		int total_rows = 0;
		sql = "select count(create_dt) as total from " + table
				+ " (index da_create_dt) where create_dt <= '" + retention
				+ "'";
		try {
			ResultSet rSet = stmt.executeQuery(sql);
			while (rSet.next())
				total_rows = rSet.getInt("total");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			log(e.getLocalizedMessage());
		}
		return total_rows;
	}

	private boolean run_select_into() {
		try {
			sql = "SELECT * INTO " + table + "_da_temp from " + table
					+ " (INDEX da_create_dt) WHERE create_dt <= '" + retention
					+ "'";
			stmt.executeUpdate(sql);
			return true;
		} catch (SQLException e) {
			int space = get_spaceused();
			if (space > 0) {
				// System.out.println("Table " + table
				// + "_da_temp contains records. Resuming Archival");
				log("Table "
						+ table
						+ " was interrupted while archiving earlier so it's been re-archived.");
				this.flag = true;
				return false;
			} else if (space <= 0) {
				// System.out.println("Table " + table
				// + "_da_temp doesn't contain any records.");
				drop_temp_table();
				run_select_into();
				return true;
			}
		}
		return false;
	}

	private boolean drop_temp_table() {
		try {
			// System.out.println("Dropping table " + table + "_da_temp");
			source_con = DriverManager.getConnection("jdbc:sybase:Tds:"
					+ source_url, source_props);
			stmt = source_con.createStatement();
			sql = "drop table " + table + "_da_temp";
			// System.out.println(sql);
			int ok = stmt.executeUpdate(sql);
			if (ok > 0) {
				stmt.close();
				source_con.close();
				return true;
			}
		} catch (SQLException e) {
			log(e.getLocalizedMessage());
			// e.printStackTrace();
		}
		return false;
	}

	int record_total;

	@Override
	public void run() {
		boolean is_big_table = false;
		try {
			if ((table.equalsIgnoreCase("dp_history")
					|| table.equalsIgnoreCase("cc_detail")
					|| table.equalsIgnoreCase("cc_pending_hist") || table
						.equalsIgnoreCase("gl_inter_detail"))
					&& option.contains("archive")) {
				boolean done = run_select_into();
				if (done) {
					is_big_table = true;
					if (option.contains("purge")) {
						status.setText("Purge thread started for" + table);
						t = new Thread() {
							@Override
							public void run() {
								run_purge();
							}
						};
						t.start();
					}
				} else
					this.flag = false;
			}
			// ///////////////////////////////////////////////////////////////////////////////////////////
			// System.out.println("Archving table " + table);
			status.setText("Archving table " + table);
			Connection sourceCon = DriverManager.getConnection(
					"jdbc:sybase:Tds:" + source_url, source_props);
			if (sourceCon != null && option.startsWith("archive")) {
				Statement stMt = sourceCon.createStatement();
				if (flag) {
					// System.out.println("Selecting from " + table +
					// "_da_temp");
					sql = "SELECT * FROM " + table
							+ "_da_temp WHERE create_dt <= '" + retention + "'";
				} else {
					// System.out.println("Selecting from " + table);
					sql = "SELECT * FROM " + table
							+ " (INDEX da_create_dt) WHERE create_dt <= '"
							+ retention + "'";
				}
				stMt.setFetchSize(800);
				rSet = stMt.executeQuery(sql);
				rMeta = rSet.getMetaData();
				dest_con = DriverManager.getConnection("jdbc:sybase:Tds:"
						+ dest_url, dest_props);
				Statement destStmt = dest_con.createStatement();
				// Retrieve the column names and datatypes
				for (int i = 1; i <= rMeta.getColumnCount(); i++) {
					columns.add(rMeta.getColumnName(i) + ";"
							+ rMeta.getColumnTypeName(i));
					if (rMeta.isAutoIncrement(i)) {
						destStmt.executeUpdate("set identity_insert " + table
								+ " ON");
					}
				}
				while (rSet.next()) {
					query = "INSERT INTO " + table + " (";
					for (String column : columns) {
						query = query + column.split(";")[0] + ",";
					}
					query = query.substring(0, query.lastIndexOf(","))
							+ ") VALUES (";
					for (String column : columns) {
						if (column.split(";")[1].equalsIgnoreCase("INT")
								|| column.split(";")[1]
										.equalsIgnoreCase("TINYINT")
								|| column.split(";")[1]
										.equalsIgnoreCase("SMALLINT")) {
							query = query + rSet.getInt(column.split(";")[0])
									+ ",";
						} else if (column.split(";")[1]
								.equalsIgnoreCase("SMALLDATETIME")) {
							Object obj = rSet.getObject(column.split(";")[0]);
							if (obj == null) {
								query = query + " "
										+ rSet.getObject(column.split(";")[0])
										+ ",";
							} else {
								query = query + "'"
										+ rSet.getObject(column.split(";")[0])
										+ "',";
							}
						} else if (column.split(";")[1]
								.equalsIgnoreCase("DECIMAL")
								|| column.split(";")[1]
										.equalsIgnoreCase("NUMERIC")) {
							query = query
									+ rSet.getBigDecimal(column.split(";")[0])
									+ ",";
						} else if (column.split(";")[1]
								.equalsIgnoreCase("CHAR")) {
							query = query + "'"
									+ rSet.getString(column.split(";")[0])
									+ "',";
						} else if (column.split(";")[1]
								.equalsIgnoreCase("VARCHAR")) {
							String value = rSet.getString(column.split(";")[0]);
							if (value != null) {
								if (value.toString().indexOf("\\") > 0) {
									value.replace("\\", "\\\\");
								}
								if (value.contains("'")) {
									value = value.replaceAll("'", " ");
								}
								query = query + "'" + value + "',";
							} else {
								query = query + "'" + value + "',";
							}
						} else {
							query = query + "'"
									+ rSet.getObject(column.split(";")[0])
									+ "',";
						}
					}
					query = query.substring(0, query.lastIndexOf(",")) + ")";
					query = query.substring(query.indexOf("I"), query.length());
					destStmt.executeUpdate(query);
					query = null;
					status.setText("Records: " + i);
					i++;
				}
				this.total_rows = i;
				record_table(table, columns.size(), i, i, table);
				columns.clear();
				query = null;
				for (int i = 1; i <= rMeta.getColumnCount(); i++) {
					columns.add(rMeta.getColumnName(i) + ";"
							+ rMeta.getColumnTypeName(i));
					if (rMeta.isAutoIncrement(i)) {
						destStmt.executeUpdate("SET IDENTITY_INSERT " + table
								+ " OFF");
					}
				}
				status.setText("Completed:" + table);
				rSet.close();
				destStmt.close();
				dest_con.close();
				// System.out.println("Completed." + table + " status" + flag);
				if (is_big_table) {
					sql = "drop table " + table + "_da_temp";
					stMt.executeUpdate(sql);
				} else if (option.contains("purge"))
					run_purge();
				else
					status.setText("Completed." + table + " status: " + flag);
				stMt.close();
				sourceCon.close();
			}
			source_con.close();
		} catch (SQLException e) {
			log(e.getMessage());
			e.printStackTrace();
		}
	}

	private int compute_batches(int int1) {
		return (int1 / 50000);
	}

	private void log(String value) {
		PrintWriter pr;
		if (!new File("config/App_logs/").exists()) {
			new File("config/App_logs").mkdirs();
		}
		File log = new File("config/App_logs/DA_log_sheet.log");
		try {
			if (log.length() > (1024 * 3058)) {
				log.delete();
			}
			pr = new PrintWriter(new FileWriter(log, true));
			pr.println("_________________________________________________");
			pr.println("Log Started at:"
					+ new SimpleDateFormat("yyyy-MM-dd hh:mm:ss"));
			pr.println(value);
			pr.println("Log ends here at: "
					+ new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
							.format(new Date()));
			pr.println("_________________________________________________");
			pr.flush();
			pr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void record_table(String table_name, int col_num, int qual_rows,
			int rows, String retention) {
		PrintWriter pr = null;
		try {
			if (!new File("config/App_logs/").exists()) {
				new File("config/App_logs").mkdirs();
			}
			File table_file = new File(
					"config/App_logs/"
							+ table_name
							+ "_"
							+ new SimpleDateFormat("yyyy_mm_dd_HH_mm_ss")
									.format(new Date()) + ".log");
			if (!table_file.exists()) {
				table_file.createNewFile();
			}
			pr = new PrintWriter(new FileWriter(table_file));
			pr.println("_________________________________________________");
			pr.println("Table Name:" + table_name);
			pr.println("Retention Period:" + retention);
			pr.println("Archive Date Column: create_dt");
			pr.println("Number of Columns:" + col_num);
			pr.println("Number of Qualified Records:" + qual_rows);
			pr.println("Number of Archived Records" + rows);
			pr.println("Total Records before Archiving: " + record_total);
			pr.println("Total Records Remaining: " + (record_total - rows));
			pr.println("_________________________________________________");
			pr.flush();
			pr.close();
		} catch (IOException ex) {
			// ex.printStackTrace();
			log(ex.getLocalizedMessage());
		}
	}
}
