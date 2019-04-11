package com.iig.gcp.schedulerserver.repository;

import com.iig.gcp.schedulerserver.dto.ScheduleExtractDto;

public interface JuniperOnPremSchedulerRepository {

	/**
	 * @param schDto
	 * @return String
	 */
	String batchExtract(ScheduleExtractDto schDto);

}
