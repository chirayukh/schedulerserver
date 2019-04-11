package com.iig.gcp.schedulerserver.dto;

import java.util.Map;

public class RequestDto {

	private Map<String, String> header;
	private Map<String, Map<String, String>> body;

	/**
	 * @return Map
	 */
	public Map<String, String> getHeader() {
		return header;
	}

	/**
	 * @param header
	 */
	public void setHeader(Map<String, String> header) {
		this.header = header;
	}

	/**
	 * @return Map
	 */
	public Map<String, Map<String, String>> getBody() {
		return body;
	}

	/**
	 * @param body
	 */
	public void setBody(Map<String, Map<String, String>> body) {
		this.body = body;
	}

}
