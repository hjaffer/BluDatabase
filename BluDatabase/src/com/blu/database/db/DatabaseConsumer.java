package com.blu.database.db;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;

public abstract class DatabaseConsumer implements Runnable {
    protected BlockingQueue<File> mFilenameQueue;
	protected Connection mConn;
	protected String mTable;
	private String mHost;
	private String mDbName;
	private String mUser;
	private String mPass;
	
	protected DatabaseConsumer() {}
	
	public DatabaseConsumer(String host, String dbName, String user, String pass,
	                        String table, BlockingQueue<File> filenameQueue) {
		this.mHost = host;
		this.mDbName = dbName;
		this.mUser = user;
		this.mPass = pass;
		this.mTable = table;
		this.mFilenameQueue = filenameQueue;
	}
	
	public void connect() throws SQLException, ClassNotFoundException {
		if (mHost.isEmpty() || mDbName.isEmpty() || mUser.isEmpty() || 
		        mPass.isEmpty()) {
			throw new SQLException("Database credentials missing");
		}
		
		Class.forName("org.postgresql.Driver");
		this.mConn = DriverManager.getConnection(this.mHost + this.mDbName, 
												 this.mUser, this.mPass);
	}
	
	public ResultSet execQuery(String query) throws SQLException {
		return this.mConn.createStatement().executeQuery(query);
	}
	
	public boolean fileAlreadyProcessed(String filename) throws SQLException {
	    String query = "select \"Log Filename\" from " + mTable + 
	                   " where \"Log Filename\" LIKE '" + filename + "'";
	    ResultSet queryResults = this.execQuery(query);
	    return queryResults.next();
	}
	
	@Override
	public void run() {
	    // method to be overridden
	}
}
