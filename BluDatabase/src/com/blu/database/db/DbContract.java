package com.blu.database.db;

public interface DbContract {
	public static final String HOST = "jdbc:postgresql://localhost:5432/";
	public static final String DB_NAME = "BluDatabase";
	public static final String USERNAME = "postgres";
	public static final String PASSWORD = "password";
	
	public static final String PCB_TESTS_TABLE = "pcb_test_logs";
	public static final String SYS_TESTS_TABLE = "sys_test_logs";
}
