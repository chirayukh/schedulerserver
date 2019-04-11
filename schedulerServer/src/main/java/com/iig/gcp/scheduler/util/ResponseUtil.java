package com.iig.gcp.scheduler.util;

public class ResponseUtil {
	/**
	 * @param status
	 * @param message
	 * @return String
	 */
	public static String createResponse(String status, String message) {

		return "{ 'status': '" + status + "','message':'" + message + "' }";
	}
}
