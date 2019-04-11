package com.iig.gcp.scheduler.repository;

import com.iig.gcp.scheduler.dto.ScheduleExtractDto;

public interface JuniperOnPremSchedulerRepository {

	/**
	 * @param schDto
	 * @return String
	 */
	String batchExtract(ScheduleExtractDto schDto);

}
