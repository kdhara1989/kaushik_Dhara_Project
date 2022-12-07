package com.cs.bean;

public class LogBean {
	
	private String eventId;
	private long duration;
	private String eventType;
	private String eventHost;
	private boolean alert;
	
	public String getEventId() {
		return eventId;
	}
	public void setEventId(String eventId) {
		this.eventId = eventId;
	}
	public long getDuration() {
		return duration;
	}
	public void setDuration(long duration) {
		this.duration = duration;
	}
	public String getEventType() {
		return eventType;
	}
	public void setEventType(String eventType) {
		this.eventType = eventType;
	}
	public String getEventHost() {
		return eventHost;
	}
	public void setEventHost(String eventHost) {
		this.eventHost = eventHost;
	}
	public boolean isAlert() {
		return alert;
	}
	public void setAlert(boolean alert) {
		this.alert = alert;
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append("{")
				.append(" eventId : ").append(getEventId())
				.append(" eventType : ").append(getEventType())
				.append(" eventHost : ").append(getEventHost())
				.append(" duration : ").append(getDuration())
				.append(" alert : ").append(isAlert())
				.append(" }")
				.toString();
	}
}
