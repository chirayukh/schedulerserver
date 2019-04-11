package com.iig.gcp.scheduler.dao;

import java.sql.Connection;
import com.iig.gcp.scheduler.dto.ScheduleExtractDto;

public interface JuniperOnPremSchedulerDao {

	/**
	 * @param conn
	 * @param schDto
	 * @return String
	 */
	String createDag(Connection conn, ScheduleExtractDto schDto);

}
