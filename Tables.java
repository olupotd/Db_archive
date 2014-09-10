/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package executor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Neptune-muk
 */
public class Tables implements Serializable {

	private static final long serialVersionUID = -2149116742899654985L;
	private List<String> tables = new ArrayList<>();

	public void setTable(List<String> table) {
		this.tables = table;
	}

	public List<String> getTables() {
		return tables;
	}
}
