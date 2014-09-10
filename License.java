package executor;

import java.io.Serializable;

public class License implements Serializable {

	private static final long serialVersionUID = -1456753193387244519L;
	private String expiry_date, bank_name, app_name;

	public String getExpiry_date() {
		return expiry_date;
	}

	public void setExpiry_date(String expiry_date) {
		this.expiry_date = expiry_date;
	}

	public String getBank_name() {
		return bank_name;
	}

	public void setBank_name(String bank_name) {
		this.bank_name = bank_name;
	}

	public License() {
		// TODO Auto-generated constructor stub
	}

	public String getApp_name() {
		return app_name;
	}

	public void setApp_name(String app_name) {
		this.app_name = app_name;
	}

}
