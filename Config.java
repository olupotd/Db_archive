/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package executor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;

public class Config {

	private FileInputStream in;
	private Properties properties;
	private String license_code;

	public void loadServers() {
		properties = new Properties();
		File propsFile = new File("config/config.xml");
		if (!propsFile.exists()) {
			JOptionPane
					.showMessageDialog(null,
							"No config File was detected but will be created with empty Configurations");
			try {
				new File("config/config.xml").createNewFile();
			} catch (IOException ex) {
				Logger.getLogger(Config.class.getName()).log(Level.SEVERE,
						null, ex);
			}
			propsFile.mkdir();
			System.exit(0);
		}

		try {
			in = new FileInputStream(propsFile);
			properties.loadFromXML(in);
			for (String key : properties.stringPropertyNames()) {
				if (key.equals("license")) {
					this.license_code = properties.getProperty(key);
				} else
					servers.add(properties.getProperty(key));
			}
		} catch (IOException ex) {
			Logger.getLogger(Config.class.getName())
					.log(Level.SEVERE, null, ex);
		}
	}

	public List<String> getServers() {
		return servers;
	}

	public List<String> getDbList() {
		return dbList;
	}

	private List<String> servers = new ArrayList<>();
	private List<String> dbList = new ArrayList<>();

	void setDbList(String string) {
		this.dbList.add(string);
	}

	public String getLicense_code() {
		return license_code;
	}

	public void setLicense_code(String license_code) {
		this.license_code = license_code;
	}
}
