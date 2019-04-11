package com.iig.gcp.schedulerserver.dto;

public class ScheduleExtractDto {

	String feed_name;
	String feed_src_type;
	String sch_flag;
	String cron;
	String file_path;
	String file_pattern;
	String kafka_topic;
	String api_unique_key;
	int project_seq;
	int feed_id;
	String extraction_mode;
	String encryption_flag;
	String country_code;

	/**
	 * @return String
	 */
	public String getCountry_code() {
		return country_code;
	}

	/**
	 * @param country_code
	 */
	public void setCountry_code(String country_code) {
		this.country_code = country_code;
	}

	/**
	 * @return String
	 */
	public String getFeed_src_type() {
		return feed_src_type;
	}

	/**
	 * @param feed_src_type
	 */
	public void setFeed_src_type(String feed_src_type) {
		this.feed_src_type = feed_src_type;
	}

	/**
	 * @return String
	 */
	public String getEncryption_flag() {
		return encryption_flag;
	}

	/**
	 * @param encryption_flag
	 */
	public void setEncryption_flag(String encryption_flag) {
		this.encryption_flag = encryption_flag;
	}

	/**
	 * @return String
	 */
	public String getExtraction_mode() {
		return extraction_mode;
	}

	/**
	 * @param extraction_mode
	 */
	public void setExtraction_mode(String extraction_mode) {
		this.extraction_mode = extraction_mode;
	}

	/**
	 * @return String
	 */
	public String getKafka_topic() {
		return kafka_topic;
	}

	/**
	 * @param kafka_topic
	 */
	public void setKafka_topic(String kafka_topic) {
		this.kafka_topic = kafka_topic;
	}

	/**
	 * @return String
	 */
	public String getApi_unique_key() {
		return api_unique_key;
	}

	/**
	 * @param api_unique_key
	 */
	public void setApi_unique_key(String api_unique_key) {
		this.api_unique_key = api_unique_key;
	}

	/**
	 * @return String
	 */
	public int getFeed_id() {
		return feed_id;
	}

	/**
	 * @param feed_id
	 */
	public void setFeed_id(int feed_id) {
		this.feed_id = feed_id;
	}

	/**
	 * @return String
	 */
	public int getProject_seq() {
		return project_seq;
	}

	/**
	 * @param project_seq
	 */
	public void setProject_seq(int project_seq) {
		this.project_seq = project_seq;
	}

	/**
	 * @return String
	 */
	public String getFeed_name() {
		return feed_name;
	}

	/**
	 * @param feed_name
	 */
	public void setFeed_name(String feed_name) {
		this.feed_name = feed_name;
	}

	/**
	 * @return String
	 */
	public String getSch_flag() {
		return sch_flag;
	}

	/**
	 * @param sch_flag
	 */
	public void setSch_flag(String sch_flag) {
		this.sch_flag = sch_flag;
	}

	/**
	 * @return String
	 */
	public String getCron() {
		return cron;
	}

	/**
	 * @param cron
	 */
	public void setCron(String cron) {
		this.cron = cron;
	}

	/**
	 * @return String
	 */
	public String getFile_path() {
		return file_path;
	}

	/**
	 * @param file_path
	 */
	public void setFile_path(String file_path) {
		this.file_path = file_path;
	}

	/**
	 * @return String
	 */
	public String getFile_pattern() {
		return file_pattern;
	}

	/**
	 * @param file_pattern
	 */
	public void setFile_pattern(String file_pattern) {
		this.file_pattern = file_pattern;
	}

}
