package com.cs.constant;

public class LogSQLConstant {

	public static final String SQL_CREATE = "CREATE TABLE IF NOT EXISTS LOG_INFO ( id VARCHAR(100) NOT NULL, host VARCHAR(100) NULL, type VARCHAR(100) NULL, duration BIGINT, alert BIT default 0, PRIMARY KEY (id));";
	public static final String SQL_INSERT = "INSERT INTO LOG_INFO VALUES (?, ?, ?, ?, ?);";
	public static final String SQL_SELECT = "SELECT * FROM LOG_INFO" ;
	public static final String SQL_DELETE = "DELETE FROM LOG_INFO";
}
