/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ui;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.swing.JButton;
import javax.swing.JOptionPane;
import javax.swing.JRootPane;
import javax.swing.SwingUtilities;

/**
 *
 * @author Neptune-muk
 */
@SuppressWarnings("serial")
public class loginFrame extends javax.swing.JFrame {

	/**
	 * Creates new form loginFrame
	 */
	JButton arc, arcpur;
	Neptune_ form;
	Main main;

	// main, this, archiveBtn, archivePurgeBtn
	public loginFrame(Main main, Neptune_ setup, JButton archivebtn,
			JButton arcPurgBtn) {

		initComponents();
		this.setLocationRelativeTo(null);
		this.form = setup;
		this.main = main;
		this.arc = archivebtn;
		this.arcpur = arcPurgBtn;
		serverURL.setText(main.getServer().remote_url);
		JRootPane rootPane = SwingUtilities.getRootPane(loginBtn);
		rootPane.setDefaultButton(loginBtn);
	}

	// <editor-fold defaultstate="collapsed"
	// desc="Generated Code">//GEN-BEGIN:initComponents
	private void initComponents() {

		jPanel1 = new javax.swing.JPanel();
		jLabel1 = new javax.swing.JLabel();
		jLabel2 = new javax.swing.JLabel();
		jLabel3 = new javax.swing.JLabel();
		serverURL = new javax.swing.JLabel();
		usernameF = new javax.swing.JTextField();
		loginBtn = new javax.swing.JButton();
		cancelBtn = new javax.swing.JButton();
		passwordF = new javax.swing.JPasswordField();

		setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

		jPanel1.setBackground(new java.awt.Color(204, 204, 204));

		jLabel1.setText("Username");

		jLabel2.setText("Password");

		jLabel3.setText("Authentication Required To Access:");

		serverURL.setText("Authentication Required To Access:");

		usernameF.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				usernameFActionPerformed(evt);
			}
		});

		loginBtn.setText("Login");
		loginBtn.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				loginBtnActionPerformed(evt);
			}
		});

		cancelBtn.setText("Cancel");
		cancelBtn.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				cancelBtnActionPerformed(evt);
			}
		});

		passwordF.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				passwordFActionPerformed(evt);
			}
		});

		javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(
				jPanel1);
		jPanel1.setLayout(jPanel1Layout);
		jPanel1Layout
				.setHorizontalGroup(jPanel1Layout
						.createParallelGroup(
								javax.swing.GroupLayout.Alignment.LEADING)
						.addGroup(
								jPanel1Layout
										.createSequentialGroup()
										.addGroup(
												jPanel1Layout
														.createParallelGroup(
																javax.swing.GroupLayout.Alignment.LEADING)
														.addGroup(
																jPanel1Layout
																		.createSequentialGroup()
																		.addGap(82,
																				82,
																				82)
																		.addComponent(
																				jLabel3)
																		.addPreferredGap(
																				javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
																		.addComponent(
																				serverURL,
																				javax.swing.GroupLayout.PREFERRED_SIZE,
																				221,
																				javax.swing.GroupLayout.PREFERRED_SIZE)
																		.addGap(0,
																				0,
																				Short.MAX_VALUE))
														.addGroup(
																jPanel1Layout
																		.createSequentialGroup()
																		.addGap(22,
																				22,
																				22)
																		.addGroup(
																				jPanel1Layout
																						.createParallelGroup(
																								javax.swing.GroupLayout.Alignment.TRAILING,
																								false)
																						.addGroup(
																								jPanel1Layout
																										.createSequentialGroup()
																										.addComponent(
																												jLabel2)
																										.addGap(18,
																												18,
																												18)
																										.addComponent(
																												passwordF))
																						.addGroup(
																								jPanel1Layout
																										.createSequentialGroup()
																										.addComponent(
																												jLabel1)
																										.addGap(18,
																												18,
																												18)
																										.addComponent(
																												usernameF,
																												javax.swing.GroupLayout.PREFERRED_SIZE,
																												278,
																												javax.swing.GroupLayout.PREFERRED_SIZE)))
																		.addPreferredGap(
																				javax.swing.LayoutStyle.ComponentPlacement.RELATED,
																				51,
																				Short.MAX_VALUE)
																		.addGroup(
																				jPanel1Layout
																						.createParallelGroup(
																								javax.swing.GroupLayout.Alignment.LEADING)
																						.addComponent(
																								loginBtn,
																								javax.swing.GroupLayout.Alignment.TRAILING,
																								javax.swing.GroupLayout.PREFERRED_SIZE,
																								133,
																								javax.swing.GroupLayout.PREFERRED_SIZE)
																						.addComponent(
																								cancelBtn,
																								javax.swing.GroupLayout.Alignment.TRAILING,
																								javax.swing.GroupLayout.PREFERRED_SIZE,
																								135,
																								javax.swing.GroupLayout.PREFERRED_SIZE))))
										.addContainerGap()));
		jPanel1Layout
				.setVerticalGroup(jPanel1Layout
						.createParallelGroup(
								javax.swing.GroupLayout.Alignment.LEADING)
						.addGroup(
								jPanel1Layout
										.createSequentialGroup()
										.addContainerGap()
										.addGroup(
												jPanel1Layout
														.createParallelGroup(
																javax.swing.GroupLayout.Alignment.BASELINE)
														.addComponent(jLabel3)
														.addComponent(serverURL))
										.addGap(24, 24, 24)
										.addGroup(
												jPanel1Layout
														.createParallelGroup(
																javax.swing.GroupLayout.Alignment.BASELINE)
														.addComponent(jLabel1)
														.addComponent(
																usernameF,
																javax.swing.GroupLayout.PREFERRED_SIZE,
																javax.swing.GroupLayout.DEFAULT_SIZE,
																javax.swing.GroupLayout.PREFERRED_SIZE)
														.addComponent(loginBtn))
										.addGap(18, 18, 18)
										.addGroup(
												jPanel1Layout
														.createParallelGroup(
																javax.swing.GroupLayout.Alignment.BASELINE)
														.addComponent(jLabel2)
														.addComponent(cancelBtn)
														.addComponent(
																passwordF,
																javax.swing.GroupLayout.PREFERRED_SIZE,
																javax.swing.GroupLayout.DEFAULT_SIZE,
																javax.swing.GroupLayout.PREFERRED_SIZE))
										.addContainerGap(23, Short.MAX_VALUE)));

		javax.swing.GroupLayout layout = new javax.swing.GroupLayout(
				getContentPane());
		getContentPane().setLayout(layout);
		layout.setHorizontalGroup(layout.createParallelGroup(
				javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				layout.createSequentialGroup()
						.addContainerGap()
						.addComponent(jPanel1,
								javax.swing.GroupLayout.PREFERRED_SIZE,
								javax.swing.GroupLayout.DEFAULT_SIZE,
								javax.swing.GroupLayout.PREFERRED_SIZE)
						.addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE,
								Short.MAX_VALUE)));
		layout.setVerticalGroup(layout.createParallelGroup(
				javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				layout.createSequentialGroup()
						.addContainerGap()
						.addComponent(jPanel1,
								javax.swing.GroupLayout.PREFERRED_SIZE,
								javax.swing.GroupLayout.DEFAULT_SIZE,
								javax.swing.GroupLayout.PREFERRED_SIZE)
						.addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE,
								Short.MAX_VALUE)));

		pack();
	}// </editor-fold>//GEN-END:initComponents

	@SuppressWarnings({ "deprecation" })
	private void loginBtnActionPerformed(java.awt.event.ActionEvent evt) {// GEN-FIRST:event_loginBtnActionPerformed
		// TODO add your handling code here:
		if (!usernameF.getText().equals("") && !passwordF.getText().equals("")) {

			try {
				Connection con = DriverManager.getConnection("jdbc:sybase:Tds:"
						+ main.getServer().getRemote_url(),
						usernameF.getText(), passwordF.getText());
				if (!con.isClosed()) {
					main.getServer().setRemote_user(usernameF.getText());
					main.getServer().setRemote_pass(passwordF.getText());
					arc.setEnabled(true);
					arcpur.setEnabled(true);
					this.setVisible(false);
				}
			} catch (SQLException ex) {
				JOptionPane.showMessageDialog(this, ex.getLocalizedMessage());
			}
		} else {
			JOptionPane.showMessageDialog(this,
					"All credentials must be filled in");
		}

	}// GEN-LAST:event_loginBtnActionPerformed

	private void cancelBtnActionPerformed(java.awt.event.ActionEvent evt) {
		this.setVisible(false);
	}

	@SuppressWarnings("deprecation")
	private void passwordFActionPerformed(java.awt.event.ActionEvent evt) {// GEN-FIRST:event_passwordFActionPerformed
		if (!usernameF.getText().equals("") && !passwordF.getText().equals("")) {
			loginBtn.setEnabled(true);
		}

		// TODO add your handling code here:
	}// GEN-LAST:event_passwordFActionPerformed

	private void usernameFActionPerformed(java.awt.event.ActionEvent evt) {// GEN-FIRST:event_usernameFActionPerformed
		// TODO add your handling code here:
	}// GEN-LAST:event_usernameFActionPerformed
		// Variables declaration - do not modify//GEN-BEGIN:variables

	private javax.swing.JButton cancelBtn;
	private javax.swing.JLabel jLabel1;
	private javax.swing.JLabel jLabel2;
	private javax.swing.JLabel jLabel3;
	private javax.swing.JPanel jPanel1;
	private javax.swing.JButton loginBtn;
	private javax.swing.JPasswordField passwordF;
	private javax.swing.JLabel serverURL;
	private javax.swing.JTextField usernameF;
	// End of variables declaration//GEN-END:variables
}
