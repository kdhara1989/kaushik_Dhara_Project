package com.cs.service;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.log4j.Logger;
import java.util.stream.Collectors;
import org.json.JSONObject;
import org.springframework.util.ResourceUtils;
import com.cs.bean.LogBean;
import com.cs.dao.LogDao;

/**
 * @author Kaushik
 *
 */
public class LogService {

	private static Logger log = Logger.getLogger(LogService.class.getName());  
	private LogDao dao = new LogDao();
	
	public void analyzeLogFile() {
		try {
			String filePath = getFilePath();
			log.debug("filePath - " + filePath);
			Map<String, List<JSONObject>> logs = readLogFile(filePath);
			List<LogBean> logInfos = processLogFile(logs);
			log.debug("Total log entries to persist - " + logInfos.size());
			dao.wirteInDB(logInfos);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
	}
	
	/**
	 * Provides path for log file
	 * 
	 * @return
	 * @throws FileNotFoundException 
	 */
	private String getFilePath() throws FileNotFoundException {
		return ResourceUtils.getFile("src/main/resources/file/logfile.txt").getAbsolutePath();
	}
	
	/**
	 * Processing file to create java objects for processing
	 * 
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	private Map<String, List<JSONObject>> readLogFile(String filePath) throws IOException {
		return Files
				.readAllLines(Paths.get(filePath))
				.parallelStream()
				.map(log-> new JSONObject(log))
				.collect(
					Collectors.groupingBy(
						obj-> obj.getString("id")
					)
				);
	}
	
	/**
	 * Processing raw log info to prepare required infos for DB persistence 
	 * 
	 * @param logs
	 * @return
	 */
	private List<LogBean> processLogFile(Map<String, List<JSONObject>> logs) {
		return Optional.ofNullable(logs)
				.orElseGet(Collections::emptyMap)
				.entrySet()
				.parallelStream()
				.map(e -> createBean(e.getValue()))
				.collect(Collectors.toList());
	}
	
	
	/**
	 * Creating bean with required log infos
	 * 
	 * @param logs
	 * @return
	 */
	private LogBean createBean(List<JSONObject> logs) {
		LogBean logBean = new LogBean();
		long startTime = 0;
		long endTime = 0;
		for(JSONObject l : logs) {
				if("STARTED".equalsIgnoreCase(l.getString("state"))) {
					startTime = l.getLong("timestamp");
				} else {
					endTime = l.getLong("timestamp");
				}
				if (!Optional.ofNullable(logBean.getEventId()).isPresent()) logBean.setEventId(l.getString("id"));
				if (!Optional.ofNullable(logBean.getEventHost()).isPresent() && l.has("host")) logBean.setEventHost(l.getString("host"));
				if (!Optional.ofNullable(logBean.getEventType()).isPresent() && l.has("type")) logBean.setEventType(l.getString("type"));
		}
		logBean.setDuration(endTime - startTime);
		if(logBean.getDuration() > 4) logBean.setAlert(Boolean.TRUE);
		return logBean;
	}
	
}
