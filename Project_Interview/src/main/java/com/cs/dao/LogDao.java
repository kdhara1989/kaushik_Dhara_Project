package com.cs.dao;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
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
	
	private JdbcTemplate jdbcTemplate;

	/**
	 * Inserting log infos to DB
	 * 
	 * @param logs
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 * @throws FileNotFoundException 
	 */
	public void wirteInDB(List<LogBean> logs) throws ClassNotFoundException, SQLException, FileNotFoundException {
		
		DataSource datasource = getDataSource();
		setJdbcTemplate(new JdbcTemplate(datasource));

		if(createTable(datasource.getConnection()) == 0 ) deleteTable(datasource.getConnection()); 

		insertLogInfos(logs);

		printTable();
	}
	
	/**
	 * Creates datasource objects for file based HSQLDB
	 * 
	 * @return
	 */
	public static DriverManagerDataSource getDataSource() throws FileNotFoundException {

		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName("org.hsqldb.jdbc.JDBCDriver");
		dataSource.setUrl("jdbc:hsqldb:" + ResourceUtils.getFile("src/main/resources/db/demodb").getAbsolutePath());
		dataSource.setUsername("sa");
		dataSource.setPassword("");
		return dataSource;
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
	private void insertLogInfos(List<LogBean> logs) {

		final int batchSize = 500;
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
	private void printTable() {
		try {
			List<LogBean> logs = getJdbcTemplate().query(SQL_SELECT, (rs, rowNum) -> {
											LogBean log = new LogBean();
											log.setEventId(rs.getString(1));
											log.setEventHost(rs.getString(2));
											log.setEventType(rs.getString(3));
											log.setDuration(rs.getLong(4));
											log.setAlert(rs.getBoolean(5));
											return log;
										});
			log.info(" ===================================== Table ======================================= ");
			log.info(" ID | HOST | TYPE | DURATION | ALERT ");
			Optional.ofNullable(logs).orElse(Collections.emptyList()).forEach( l-> {
				log.info(l.getEventId() + " | "+ l.getEventHost() + " | " + l.getEventType() + " | " + l.getDuration() + " | " + l.isAlert());
			});
			log.info(" =================================================================================== ");

		}  catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}

	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}
	
	
}
