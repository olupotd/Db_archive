package cons;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class BatchInsert implements Runnable {

	// ip:port/db;user;pass|ip:port/db;user;pass
	private String table, retention, option, sql, query;
	private Connection source_con, dest_con;
	private String source_url, dest_url;
	private Properties source_props, dest_props;
	ResultSet rSet = null;
	ResultSetMetaData rMeta;
	List<String> columns;
	int rowcounts;
	private PreparedStatement prepared;
	private Statement stmt;
	int i;
	private boolean flag = false;

	public BatchInsert(String tab_props, String option, String urls,
			String logins) {
		super();
		try {
			columns = new ArrayList<>();
			source_props = new Properties();
			dest_props = new Properties();
			this.table = tab_props.split(";")[0];
			this.retention = tab_props.split(";")[1];
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
			// check for the size of this table
			int size = check_size(table);
			if (size >= 5000000 && option.contains("archive")) {
				System.out.println("table " + table + " Contains " + size
						+ " records");
				flag = true;
				boolean done = run_select_into();
				System.out.println("Completed selecting into" + table);
				if (done && option.contains("purge")) {
					flag = true;
					Thread t = new Thread() {
						@Override
						public void run() {
							run_purge();
						}
					};
					t.start();
				} else if (done)
					flag = true;
				else
					flag = false;

			} else if (option.equalsIgnoreCase("purge")) {
				run_purge();
				// System.out.println("completed Purge on " + table);
			}
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private int compute_batches(int int1) {
		return (int1 / 50000);
	}

	private void run_purge() {
		try {
			System.out.println("Started Purge on " + table);
			Connection sourceCon = DriverManager.getConnection(
					"jdbc:sybase:Tds:" + source_url, source_props);
			if (!sourceCon.isClosed()) {
				Statement sourceStmt = sourceCon.createStatement();
				sourceCon.setAutoCommit(true);
				sql = "SELECT COUNT(create_dt) AS result FROM " + table
						+ " (INDEX da_create_dt) WHERE create_dt <= '"
						+ retention + "'";
				rSet = sourceStmt.executeQuery(sql);
				while (rSet.next()) {
					rowcounts = compute_batches(rSet.getInt("result"));
				}
				int count = 0;
				while (count <= rowcounts) {
					sql = "SET ROWCOUNT 50000 DELETE " + table + " FROM "
							+ table
							+ " (INDEX da_create_dt) WHERE create_dt <= '"
							+ retention + "'";
					sourceStmt.executeUpdate(sql);
					// System.out.println("Dumping tran with no_log");
					sourceStmt.executeUpdate("dump tran "
							+ source_url.split("/")[1] + " with no_log");
					// System.out.println("dumping tran on tempdb");
					sourceStmt.executeUpdate("dump tran tempdb with no_log");
					count++;
					// System.out.println("Purged: " + count);
				}
				sourceStmt.executeUpdate("update index statistics " + table
						+ " da_create_dt with sampling=20 percent");
				System.out.println("Purged: " + table);
				rSet.close();
				sourceStmt.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
			// log(e.getLocalizedMessage());
		}
	}

	private boolean run_select_into() {
		try {
			// System.out.println("Running Select into " + table);
			sql = "SELECT * INTO " + table + "_temp from " + table
					+ " (INDEX da_create_dt) WHERE create_dt <= '" + retention
					+ "'";
			int ok = stmt.executeUpdate(sql);
			if (ok > 0) {
				stmt.executeUpdate("dump tran " + source_url.split("/")[1]
						+ " with no_log");
				stmt.executeUpdate("dump tran tempdb with no_log");
				return true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	private boolean drop_temp_table() {
		try {
			source_con = DriverManager.getConnection("jdbc:sybase:Tds:"
					+ source_url, source_props);
			stmt = source_con.createStatement();
			System.out.println("Dropping temp table " + table + "_temp");
			sql = "drop table " + table + "_temp";
			int ok = stmt.executeUpdate(sql);
			if (ok > 0) {
				source_con.close();
				return true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	private int check_size(String table2) {
		int total = 0;
		try {
			ResultSet results = stmt.executeQuery("sp_spaceused " + table2);
			while (results.next())
				total = results.getInt("rowtotal");
			results.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return total;
	}

	@Override
	public void run() {
		try {
			System.out.println("Running Main thread for " + table);
			Connection sourceCon = DriverManager.getConnection(
					"jdbc:sybase:Tds:" + source_url, source_props);
			if (sourceCon != null && option.startsWith("archive")) {
				Statement stMt = sourceCon.createStatement();
				if (flag) {
					// System.out.println("Selecting.." + table + "_temp");
					sql = "SELECT * FROM " + table
							+ "_temp  WHERE create_dt <= '" + retention + "'";
				} else {
					// System.out.println("Selecting.." + table);
					sql = "SELECT * FROM " + table
							+ " (INDEX da_create_dt) WHERE create_dt <= '"
							+ retention + "'";
				}
				stMt.setFetchSize(600); //
				rSet = stMt.executeQuery(sql);
				rMeta = rSet.getMetaData();
				int colcount = rMeta.getColumnCount();
				dest_con = DriverManager.getConnection("jdbc:sybase:Tds:"
						+ dest_url, dest_props);
				// dest_con.setAutoCommit(false);
				System.out.println("Connected to Destination");
				String set_insert_on = "set identity_insert " + table + " ON";
				query = "INSERT INTO " + table + "(";
				String q = " VALUES (";
				prepared = dest_con.prepareStatement(set_insert_on);
				for (int i = 1; i <= colcount; i++) {
					columns.add(rMeta.getColumnName(i) + ";"
							+ rMeta.getColumnTypeName(i));
					System.out.println("Table " + rMeta.getColumnName(i));
					if (rMeta.isAutoIncrement(i)) {
						prepared.executeUpdate();
						// dest_con.commit();
					}
					query = query + rMeta.getColumnName(i) + ",";
					q = q + "?,";
				}
				query = query.substring(0, query.lastIndexOf(",")) + ")";
				String Query = query + q.substring(0, q.lastIndexOf(",")) + ")";
				int rows = 1, count = 0, num = 0;
				prepared = dest_con.prepareStatement(Query);
				while (rSet.next()) {
					for (String column : columns) {
						if (column.split(";")[1].equalsIgnoreCase("INT")) {
							prepared.setInt(rows,
									rSet.getInt(column.split(";")[0]));
						} else if (column.split(";")[1]
								.equalsIgnoreCase("TINYINT")) {
							prepared.setByte(rows,
									rSet.getByte(column.split(";")[0]));
						} else if (column.split(";")[1]
								.equalsIgnoreCase("SMALLINT")) {
							prepared.setShort(rows,
									rSet.getShort(column.split(";")[0]));
						} else if (column.split(";")[1]
								.equalsIgnoreCase("SMALLDATETIME")) {
							prepared.setTime(rows,
									rSet.getTime(column.split(";")[0]));
						} else if (column.split(";")[1]
								.equalsIgnoreCase("DECIMAL")
								|| column.split(";")[1]
										.equalsIgnoreCase("NUMERIC")) {
							prepared.setBigDecimal(rows,
									rSet.getBigDecimal(column.split(";")[0]));
						} else if (column.split(";")[1]
								.equalsIgnoreCase("CHAR")) {
							prepared.setString(rows,
									rSet.getString(column.split(";")[0]));
						} else if (column.split(";")[1]
								.equalsIgnoreCase("VARCHAR")) {
							String value = rSet.getString(column.split(";")[0]);
							if (!rSet.wasNull()) {
								if (value.toString().indexOf("\\") > 0) {
									value.replace("\\", "\\\\");
								}
								if (value.contains("'")) {
									value = value.replaceAll("'", " ");
								}
								prepared.setString(rows, value);
							} else {
								prepared.setString(rows, "NULL");
							}
						} else {
							prepared.setObject(rows,
									rSet.getObject(column.split(";")[0]));
						}
						rows++;
					}
					// prepared.addBatch();
					prepared.execute();
					rows = 1;
					count++;
					if (count % 1000 == 0) {
						// prepared.executeBatch();
						// dest_con.commit();
						num++;
						System.out.println((num * count) + " for " + table);
						count = 0;
					}
					i++;
				}
				prepared.executeBatch();
				dest_con.commit();
				record_table(table, columns.size(), i, i, retention);
				System.out.println("Completed:" + table);
				rSet.close();
				stmt.close();
				dest_con.close();
				if (flag)
					drop_temp_table();
				else if (option.contains("purge"))
					run_purge();
				else {
					sourceCon.close();
					dest_con.close();
				}
			} else if (option.equals("drop_temp")) {
				drop_temp_table();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (source_con != null)
					source_con.close();
				columns.clear();
				query = null;
			} catch (SQLException sql) {
				sql.printStackTrace();
			}
		}

	}

	/*
	 * Thread t = new Thread() {
	 * 
	 * @Override public void run() { System.out.println("Running Purge on " +
	 * table); run_purge(); } }; t.start();
	 */

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
							+ new SimpleDateFormat("yyyy_mm_dd_HH_mm_ss")
									.format(new Date()) + ".log");
			if (!table_file.exists()) {
				table_file.createNewFile();
			}
			pr = new PrintWriter(new FileWriter(table_file));
			pr.println("_________________________________________________");
			pr.println("Table Name:" + table_name);
			pr.println("Retention Period:" + retention);
			pr.println("Archive Date Column:" + "create_dt");
			pr.println("Number of Columns:" + col_num);
			pr.println("Number of Qualified Records:" + qual_rows);
			pr.println("Number of Archived Records" + rows);
			pr.println("_________________________________________________");
			pr.flush();
			pr.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

}
