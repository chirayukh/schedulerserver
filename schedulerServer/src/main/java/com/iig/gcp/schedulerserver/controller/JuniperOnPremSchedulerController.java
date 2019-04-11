package com.iig.gcp.schedulerserver.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.iig.gcp.schedulerserver.dto.RequestDto;
import com.iig.gcp.schedulerserver.dto.ScheduleExtractDto;
import com.iig.gcp.schedulerserver.repository.JuniperOnPremSchedulerRepository;
import com.iig.gcp.schedulerserver.util.ResponseUtil;

@CrossOrigin
@RestController
public class JuniperOnPremSchedulerController {

	@Autowired
	JuniperOnPremSchedulerRepository Repository;

	/**
	 * @param requestDto
	 * @return String
	 * @throws UnsupportedOperationException
	 * @throws Exception
	 */
	@RequestMapping(value = "/createDag", method = RequestMethod.POST)
	@ResponseBody
	public String createDag(@RequestBody RequestDto requestDto) throws UnsupportedOperationException, Exception {
		String response="";
		String status="";
		String message="";
		ScheduleExtractDto schDto=new ScheduleExtractDto();
		schDto.setFeed_name(requestDto.getBody().get("data").get("feed_name"));
		schDto.setSch_flag(requestDto.getBody().get("data").get("sch_flag"));
		schDto.setExtraction_mode(requestDto.getBody().get("data").get("extraction_mode"));
		if(schDto.getSch_flag().equalsIgnoreCase("R")) {
			schDto.setCron(requestDto.getBody().get("data").get("cron"));
		}
		if(schDto.getSch_flag().equalsIgnoreCase("F")) {
			schDto.setFile_path(requestDto.getBody().get("data").get("file_path"));
			schDto.setFile_pattern(requestDto.getBody().get("data").get("file_pattern"));
		}
		if(schDto.getSch_flag().equalsIgnoreCase("K")) {

			schDto.setKafka_topic(requestDto.getBody().get("data").get("kafka_topic"));	
		}
		if(schDto.getSch_flag().equalsIgnoreCase("A")) {
			schDto.setApi_unique_key(requestDto.getBody().get("data").get("api_unique_key"));
		}
		try {
			response=Repository.batchExtract(schDto);
			if(response.equalsIgnoreCase("success")) {
				status="Success";
				message="Batch Scheduled Successfully";
			}
			else {
				status="Failed";
				message=response;
			}

		} catch (Exception e) {
			e.printStackTrace();
			status="Failed";
			message=e.getMessage();
		}

		// Parse Json to Dto Object
		return ResponseUtil.createResponse(status, message);
	}

}
