package com.iig.gcp.schedulerserver.dao;

import java.sql.Connection;

import com.iig.gcp.schedulerserver.dto.ScheduleExtractDto;

public interface JuniperOnPremSchedulerDao {

	/**
	 * @param conn
	 * @param schDto
	 * @return String
	 */
	String createDag(Connection conn, ScheduleExtractDto schDto);

}
