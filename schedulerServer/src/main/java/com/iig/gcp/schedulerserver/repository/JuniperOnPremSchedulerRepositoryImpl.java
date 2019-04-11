package com.iig.gcp.schedulerserver.repository;

import java.sql.Connection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.iig.gcp.schedulerserver.dao.JuniperOnPremSchedulerDao;
import com.iig.gcp.schedulerserver.dto.ScheduleExtractDto;
import com.iig.gcp.schedulerserver.util.MetadataDBConnectionUtils;

//For establishing connection to Metadata
@Component
public class JuniperOnPremSchedulerRepositoryImpl implements JuniperOnPremSchedulerRepository {

	@Autowired
	JuniperOnPremSchedulerDao Dao;

	/**
	 * @param schdto
	 * @return String
	 */
	@Override
	public String batchExtract(ScheduleExtractDto schDto) {
		Connection conn = null;
		try {
			conn = MetadataDBConnectionUtils.getOracleConnection();

		} catch (Exception e) {
			e.printStackTrace();
			return "Failed to connect to Metadata database";
		}

		return Dao.createDag(conn, schDto);
	}

}
