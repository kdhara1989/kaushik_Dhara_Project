package com.cs.dao;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.util.ResourceUtils;
import com.cs.bean.LogBean;
import static com.cs.constant.LogSQLConstant.SQL_CREATE;
import static com.cs.constant.LogSQLConstant.SQL_INSERT;
import static com.cs.constant.LogSQLConstant.SQL_SELECT;
import static com.cs.constant.LogSQLConstant.SQL_DELETE;

/**
 * @author Kaushik
 *
 */
public class LogDao {
	
	static Logger log = Logger.getLogger(LogDao.class.getName()); 
	
	/**
	 * Inserting log infos to DB
	 * 
	 * @param logs
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 */
	public void wirteInDB(List<LogBean> logs) throws ClassNotFoundException, SQLException {
		Connection con = createConnection();
		
		if(createTable(con) == 0 ) deleteTable(con); 
		
		insertLogInfos(con, logs);
		 
		printTable(con);
		closeConnection(con);
	}
	
	
	/**
	 * Creates connection objects for file based HSQLDB
	 * 
	 * @return
	 */
	private Connection createConnection() {
		Connection con = null;
		try {
			Class.forName("org.hsqldb.jdbc.JDBCDriver");
			con = DriverManager.getConnection("jdbc:hsqldb:" + ResourceUtils.getFile("src/main/resources/db/demodb").getAbsolutePath(), "SA", "");
			if (con!= null){
				log.info("Connection created successfully");
			}else{
				log.info("Problem with creating connection");
			}
		} catch (ClassNotFoundException | SQLException | FileNotFoundException e) {
			log.error(e.getMessage(), e);
		}
		return con;
	}
	
	/**
	 * Closing connection
	 * 
	 * @param con
	 */
	private void closeConnection(Connection con) {
		try {
			con.close();
		} catch (SQLException e) {
			log.error(e.getMessage(), e);
		}
	}
	
	/**
	 * Creates table if not exist
	 * 
	 * @param con
	 * @return
	 */
	private int createTable(Connection con) {
		Statement stmt = null;
		int result = 0;
		try {
			stmt = con.createStatement();
			result = stmt.executeUpdate(SQL_CREATE);
			if(result > 0) log.info("Table created successfully");
		}  catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}
	
	
	/**
	 * Cleans table to get it ready for next load
	 * 
	 * @param con
	 * @return
	 */
	private int deleteTable(Connection con) {
		Statement stmt = null;
		int result = 0;
		try {
			stmt = con.createStatement();
			result = stmt.executeUpdate(SQL_DELETE);
			if(result > 0) log.info("Table refreshed successfully");
		}  catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}
	
	
	/**
	 * Insert all log records in batch
	 * 
	 * @param con
	 * @param logs
	 */
	private void insertLogInfos(Connection con, List<LogBean> logs) {
		
		final int batchSize = 500;
		JdbcTemplate jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(con, false));
	    for (int j = 0; j < logs.size(); j += batchSize) {

	        final List<LogBean> batchList = logs.subList(j, j + batchSize > logs.size() ? logs.size() : j + batchSize);
	        
	        log.debug("Batch : Size - " + batchList.size() + " Start - " + j + " End - " + (j + batchList.size()));

	        jdbcTemplate.batchUpdate(SQL_INSERT,
	            new BatchPreparedStatementSetter() {
	                @Override
	                public void setValues(PreparedStatement ps, int i)
	                        throws SQLException {
	                	LogBean log = batchList.get(i);
	                    ps.setString(1, log.getEventId());
	                    ps.setString(2, log.getEventHost());
	                    ps.setString(3, log.getEventType());
	                    ps.setLong(4, log.getDuration());
	                    ps.setBoolean(5, log.isAlert());
	                }

	                @Override
	                public int getBatchSize() {
	                    return batchList.size();
	                }
	            });
	    }
	    log.info("Insertion successfull, tolat log proccessed - " + logs.size());
	}
	
	
	/**
	 * Optional - This method to print infos stored in DB table.
	 * 
	 * @param con
	 */
	private void printTable(Connection con) {
		Statement stmt = null;
		try {
			stmt = con.createStatement();
			ResultSet result = stmt.executeQuery(SQL_SELECT);

			Optional.ofNullable(result).ifPresent( r -> {
				log.info(" ===================================== Table ======================================= ");
				log.info(" ID | HOST | TYPE | DURATION | ALERT ");
				try {
					while (r.next()) {
						log.info(result.getString(1) + " | "+ result.getString(2) + " | " + result.getString(3) + " | " +  result.getLong(4) + " | " +  result.getBoolean(5));
					}
				} catch (SQLException e) {
					log.error(e.getMessage(), e);
				}
				log.info(" =================================================================================== ");
			});

		}  catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}
}
