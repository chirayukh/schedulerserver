package com.iig.gcp.schedulerserver.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;

import org.springframework.stereotype.Component;

import com.iig.gcp.schedulerserver.constant.MetadataDBConstants;
import com.iig.gcp.schedulerserver.constant.SchedulerConstants;
import com.iig.gcp.schedulerserver.dto.ScheduleExtractDto;

@Component
public class JuniperOnPremSchedulerDaoImpl implements JuniperOnPremSchedulerDao {

	/**
	 * @param conn
	 * @param schDto
	 * @return String
	 */
	/*
	 * @see com.iig.gcp.scheduler.dao.JuniperOnPremSchedulerDao#createDag(java.sql.Connection, com.iig.gcp.scheduler.dto.ScheduleExtractDto)
	 */
	@Override
	public String createDag(Connection conn, ScheduleExtractDto schDto) {

		ArrayList<String> tarArr = new ArrayList<String>();

		try {
			String feedId = getFeedId(conn, schDto.getFeed_name());
			schDto.setFeed_id(Integer.parseInt(feedId.split("~")[0]));
			schDto.setProject_seq(Integer.parseInt(feedId.split("~")[1]));
			schDto.setCountry_code(feedId.split("~")[2]);
			schDto.setFeed_src_type(feedId.split("~")[3]);
		} catch (Exception e) {
			e.printStackTrace();
			return "Error while retrieving Feed Id";
		}
		try {
			tarArr = getTarObj(conn, schDto.getFeed_name(), schDto.getFeed_id());
		} catch (Exception e) {
			e.printStackTrace();
			return "Error while retrieving target details";
		}
		if (schDto.getSch_flag().equalsIgnoreCase("O")) {
			try {
				return scheduleOnDemandFeed(conn, schDto, tarArr);
			} catch (Exception e) {
				e.printStackTrace();
				return (e.getMessage());
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		if (schDto.getSch_flag().equalsIgnoreCase("R")) {
			try {
				return insertScheduleMetadata(conn, schDto, tarArr);
			} catch (Exception e) {
				e.printStackTrace();
				return (e.getMessage());
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		if (schDto.getSch_flag().equalsIgnoreCase("F")) {
			try {
				return scheduleEventBasedFeedFile(conn, schDto, tarArr);
			} catch (Exception e) {
				e.printStackTrace();
				return e.getMessage();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		if (schDto.getSch_flag().equalsIgnoreCase("K")) {
			try {
				return scheduleEventBasedFeedKafka(conn, schDto, tarArr);
			} catch (Exception e) {
				e.printStackTrace();
				return e.getMessage();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		if (schDto.getSch_flag().equalsIgnoreCase("A")) {
			try {
				return scheduleEventBasedFeedApi(conn, schDto, tarArr);
			} catch (Exception e) {
				e.printStackTrace();
				return e.getMessage();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		} else {
			return "Invalid batch type";
		}
	}

	/**
	 * @param conn
	 * @param feed_name
	 * @return String
	 * @throws Exception
	 */
	private String getFeedId(Connection conn, String feed_name) throws Exception {

		String query = "select feed_sequence,project_sequence,country_code from " + MetadataDBConstants.FEEDTABLE
				+ " where feed_unique_name='" + feed_name + "'";
		StringBuffer feed_id = new StringBuffer();

		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(query);
		if (rs.isBeforeFirst()) {
			rs.next();
			feed_id.append(rs.getInt(1) + "~" + rs.getInt(2) + "~" + rs.getString(3) + "~");
			String query2 = "select distinct c.src_conn_type from " + MetadataDBConstants.CONNECTIONTABLE
					+ " c inner join " + MetadataDBConstants.FEEDSRCTGTLINKTABLE
					+ " l on c.src_conn_sequence=l.src_conn_sequence " + "where l.feed_sequence="
					+ feed_id.toString().split("~")[0];
			Statement stmt = conn.createStatement();
			ResultSet rs2 = stmt.executeQuery(query2);
			if (rs2.isBeforeFirst()) {
				rs2.next();
				feed_id.append(rs2.getString(1));
			}
		}
		return feed_id.toString();
	}

	/**
	 * @param conn
	 * @param feed_name
	 * @param feed_sequence
	 * @return ArrayList <String>
	 * @throws Exception
	 */

	private ArrayList<String> getTarObj(Connection conn, String feed_name, int feed_sequence) throws Exception {
		ArrayList<String> tarArr = new ArrayList<String>();
		String getTargetList = "select t.target_unique_name,t.materialization_flag from "
				+ MetadataDBConstants.TAREGTTABLE + " t inner join " + MetadataDBConstants.FEEDSRCTGTLINKTABLE
				+ " l on l.target_sequence=t.target_conn_sequence  where l.feed_sequence=" + feed_sequence;
		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(getTargetList);
		if (rs.isBeforeFirst()) {
			while (rs.next()) {
				tarArr.add(rs.getString(1) + "~" + rs.getString(2));
			}
		}
		return tarArr;
	}

	/**
	 * @param conn
	 * @param schDto
	 * @param tarArr
	 * @return String
	 * @throws Exception
	 */
	private String scheduleOnDemandFeed(Connection conn, ScheduleExtractDto schDto, ArrayList<String> tarArr)
			throws Exception {

		String insertReadJob = MetadataDBConstants.INSERTQUERY.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
				.replace("{$columns}",
						"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,schedule_type,job_schedule_time,daily_flag,project_id")
				.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_id()
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ SchedulerConstants.read_script + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~" + schDto.getFeed_src_type()
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ schDto.getExtraction_mode() + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + "O" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + "00:00:00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ schDto.getProject_seq());
		try {
			Statement statement = conn.createStatement();
			statement.executeQuery(insertReadJob);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("Exception occured while scheduling Read job");
		}
		String[] jobNames = new String[10];
		Arrays.fill(jobNames, "");
		int index = 0;
		for (String target : tarArr) {
			String insertWriteJob = MetadataDBConstants.INSERTQUERY
					.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
					.replace("{$columns}",
							"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,schedule_type,job_schedule_time,daily_flag,project_id")
					.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_" + target.split("~")[0]
							+ "_write" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
							+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
							+ schDto.getFeed_name() + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + schDto.getFeed_id() + MetadataDBConstants.QUOTE
							+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + SchedulerConstants.write_script
							+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
							+ schDto.getFeed_name() + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + target.split("~")[0] + MetadataDBConstants.QUOTE
							+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
							+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
							+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + "O" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + "00:00:00" + MetadataDBConstants.QUOTE
							+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
							+ MetadataDBConstants.COMMA + schDto.getProject_seq());
			try {
				Statement statement = conn.createStatement();
				statement.executeQuery(insertWriteJob);
			} catch (Exception e) {
				e.printStackTrace();
				throw new Exception("Exception occured while scheduling write job");
			}
			if (target.split("~")[1].equalsIgnoreCase("Y")) {
				jobNames[index] = schDto.getFeed_name() + "_" + target.split("~")[0] + "_materialize";
				String insertMatJob = MetadataDBConstants.INSERTQUERY
						.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
						.replace("{$columns}",
								"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,schedule_type,job_schedule_time,daily_flag,project_id")
						.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
								+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
								+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name()
								+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
								+ schDto.getFeed_id() + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
								+ MetadataDBConstants.QUOTE + SchedulerConstants.mat_script + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name()
								+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
								+ target.split("~")[0] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
								+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
								+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "O"
								+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
								+ "00:00:00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
								+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + schDto.getProject_seq());
				try {
					Statement statement = conn.createStatement();
					statement.executeQuery(insertMatJob);
				} catch (Exception e) {
					e.printStackTrace();
					throw new Exception("Exception occured while scheduling materialization job");
				}
			} else {
				jobNames[index] = schDto.getFeed_name() + "_" + target.split("~")[0] + "_write";
			}
			index++;
		}
		String insertDeleteJob = MetadataDBConstants.INSERTQUERY.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
				.replace("{$columns}",
						"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
								+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
								+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
								+ ",schedule_type,job_schedule_time,daily_flag,project_id")
				.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ schDto.getFeed_name() + "_delete" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_id()
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ SchedulerConstants.delete_script + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~" + schDto.getCountry_code()
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ jobNames[0] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + jobNames[1] + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[2]
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ jobNames[3] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ jobNames[6] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + jobNames[7] + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[8]
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ jobNames[9] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + "O" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + "00:00:00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ schDto.getProject_seq());
		try {
			Statement statement = conn.createStatement();
			statement.executeQuery(insertDeleteJob);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("Exception occured while scheduling delete job");
		}
		return "success";
	}

	/**
	 * @param conn
	 * @param schDto
	 * @param tarArr
	 * @return String
	 * @throws Exception
	 */
	private String insertScheduleMetadata(Connection conn, ScheduleExtractDto schDto, ArrayList<String> tarArr)
			throws Exception {

		Statement statement = conn.createStatement();
		String insertQuery = "";
		String hourlyFlag = "";
		String dailyFlag = "";
		String monthlyFlag = "";
		String weeklyFlag = "";
		String[] temp = schDto.getCron().split(" ");
		String minutes = temp[0];
		String hours = temp[1];
		String dates = temp[2];
		String months = temp[3];
		String daysOfWeek = temp[4];
		String[] jobNames = new String[10];
		Arrays.fill(jobNames, "");
		int index = 0;
		if (hours.equals("*") && dates.equals("*") && months.equals("*") && (daysOfWeek.equals("*"))) {
			hourlyFlag = "Y";
		}
		if (dates.equals("*") && months.equals("*") && daysOfWeek.equals("*") && !hours.equals("*")
				&& !minutes.equals("*")) {

			dailyFlag = "Y";
		}
		if (months.equals("*") && daysOfWeek.equals("*") && !dates.equals("*") && !hours.equals("*")
				&& !minutes.equals("*")) {

			monthlyFlag = "Y";
		}
		if (dates.equals("*") && months.equals("*") && !minutes.equals("*") && !hours.equals("*")
				&& !daysOfWeek.equals("*")) {
			weeklyFlag = "Y";

		}
		if (dailyFlag.equalsIgnoreCase("Y")) {
			if (hours.contains(",")) {
				for (String hour : hours.split(",")) {

					if (minutes.contains(",")) {
						for (String minute : minutes.split(",")) {
							insertQuery = MetadataDBConstants.INSERTQUERY
									.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
									.replace("{$columns}",
											"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,daily_flag,job_schedule_time,schedule_type,project_id")
									.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
											+ schDto.getFeed_src_type() + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour + ":"
											+ minute + ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + schDto.getProject_seq());

							statement.execute(insertQuery);
							for (String target : tarArr) {

								String insertWriteJob = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,daily_flag,job_schedule_time,schedule_type,project_id")
										.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
												+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
												+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + SchedulerConstants.write_script
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + target.split("~")[0]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
												+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + hour + ":" + minute + ":00"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + schDto.getProject_seq());

								statement.execute(insertWriteJob);

								if (target.split("~")[1].equalsIgnoreCase("Y")) {

									jobNames[index] = schDto.getFeed_name() + "_materialize";

									String insertMatJob = MetadataDBConstants.INSERTQUERY
											.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
											.replace("{$columns}",
													"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,daily_flag,job_schedule_time,schedule_type,project_id")
											.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
													+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_" + target.split("~")[0]
													+ "_materialize" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_id() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ SchedulerConstants.mat_script + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ target.split("~")[0] + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_write"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour + ":"
													+ minute + ":00" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "R"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ schDto.getProject_seq());

									statement.execute(insertMatJob);
								} else {
									jobNames[index] = schDto.getFeed_name() + "_" + target.split("~")[0] + "_write";
								}
								index++;
							}
							String insertDeleteJob = MetadataDBConstants.INSERTQUERY
									.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
									.replace("{$columns}",
											"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
													+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
													+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
													+ ",schedule_type,job_schedule_time,daily_flag,project_id")
									.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
											+ schDto.getCountry_code() + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[0] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[1]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[2] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[3]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[6] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[7]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[8] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[9]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour + ":"
											+ minute + ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + schDto.getProject_seq());
							statement.executeQuery(insertDeleteJob);
						}
					} else {
						insertQuery = MetadataDBConstants.INSERTQUERY
								.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
								.replace("{$columns}",
										"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,daily_flag,job_schedule_time,schedule_type,project_id")
								.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_id()
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
										+ schDto.getFeed_src_type() + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour + ":" + minutes
										+ ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + schDto.getProject_seq());

						statement.execute(insertQuery);
						for (String target : tarArr) {

							String insertWriteJob = MetadataDBConstants.INSERTQUERY
									.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
									.replace("{$columns}",
											"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,daily_flag,job_schedule_time,schedule_type,project_id")
									.replace("{$data}",
											MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
													+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + SchedulerConstants.write_script
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + target.split("~")[0]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + hour + ":" + minutes + ":00"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + schDto.getProject_seq());

							statement.execute(insertWriteJob);

							if (target.split("~")[1].equalsIgnoreCase("Y")) {

								jobNames[index] = schDto.getFeed_name() + "_materialize";

								String insertMatJob = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,daily_flag,job_schedule_time,schedule_type,project_id")
										.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
												+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
												+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_materialize"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + SchedulerConstants.mat_script
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + target.split("~")[0]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
												+ schDto.getFeed_name() + "_write" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + hour + ":" + minutes + ":00"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + schDto.getProject_seq());

								statement.execute(insertMatJob);
							} else {
								jobNames[index] = schDto.getFeed_name() + "_" + target.split("~")[0] + "_write";
							}
							index++;
						}
						String insertDeleteJob = MetadataDBConstants.INSERTQUERY
								.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
								.replace("{$columns}",
										"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
												+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
												+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
												+ ",schedule_type,job_schedule_time,daily_flag,project_id")
								.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_id()
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
										+ schDto.getCountry_code() + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[0] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[1]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[2] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[3]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[6] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[7]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[8] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[9]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour + ":" + minutes
										+ ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + schDto.getProject_seq());

						statement.executeQuery(insertDeleteJob);

					}
				}
			} else {
				if (minutes.contains(",")) {
					for (String minute : minutes.split(",")) {
						insertQuery = MetadataDBConstants.INSERTQUERY
								.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
								.replace("{$columns}",
										"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,daily_flag,job_schedule_time,schedule_type,project_id")
								.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_id()
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
										+ schDto.getFeed_src_type() + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours + ":" + minute
										+ ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + schDto.getProject_seq());

						statement.execute(insertQuery);
						for (String target : tarArr) {

							String insertWriteJob = MetadataDBConstants.INSERTQUERY
									.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
									.replace("{$columns}",
											"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,daily_flag,job_schedule_time,schedule_type,project_id")
									.replace("{$data}",
											MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
													+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + SchedulerConstants.write_script
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + target.split("~")[0]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + hours + ":" + minute + ":00"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + schDto.getProject_seq());

							statement.execute(insertWriteJob);

							if (target.split("~")[1].equalsIgnoreCase("Y")) {
								jobNames[index] = schDto.getFeed_name() + "_materialize";

								String insertMatJob = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,daily_flag,job_schedule_time,schedule_type,project_id")
										.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
												+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
												+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_materialize"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + SchedulerConstants.mat_script
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + target.split("~")[0]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
												+ schDto.getFeed_name() + "_write" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + hours + ":" + minute + ":00"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + schDto.getProject_seq());

								statement.execute(insertMatJob);
							} else {
								jobNames[index] = schDto.getFeed_name() + "_" + target.split("~")[0] + "_write";
							}
							index++;
						}
						String insertDeleteJob = MetadataDBConstants.INSERTQUERY
								.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
								.replace("{$columns}",
										"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
												+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
												+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
												+ ",schedule_type,job_schedule_time,daily_flag,project_id")
								.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_id()
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
										+ schDto.getCountry_code() + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[0] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[1]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[2] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[3]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[6] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[7]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[8] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[9]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours + ":" + minute
										+ ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + schDto.getProject_seq());

						statement.executeQuery(insertDeleteJob);

					}
				} else {
					insertQuery = MetadataDBConstants.INSERTQUERY.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
							.replace("{$columns}",
									"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,daily_flag,job_schedule_time,schedule_type,project_id")
							.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
									+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
									+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE
									+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name()
									+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
									+ schDto.getFeed_id() + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
									+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
									+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
									+ schDto.getFeed_name() + "~" + schDto.getFeed_src_type()
									+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
									+ "$RUN_ID$" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
									+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
									+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
									+ "Y" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
									+ MetadataDBConstants.QUOTE + hours + ":" + minutes + ":00"
									+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
									+ "R" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
									+ schDto.getProject_seq());

					statement.execute(insertQuery);

					for (String target : tarArr) {

						String insertWriteJob = MetadataDBConstants.INSERTQUERY
								.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
								.replace("{$columns}",
										"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,daily_flag,job_schedule_time,schedule_type,project_id")
								.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
										+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name()
										+ "_" + target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name()
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_id() + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
										+ SchedulerConstants.write_script + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name()
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + target.split("~")[0] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours + ":" + minutes
										+ ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + schDto.getProject_seq());

						statement.execute(insertWriteJob);

						if (target.split("~")[1].equalsIgnoreCase("Y")) {

							jobNames[index] = schDto.getFeed_name() + "_materialize";

							String insertMatJob = MetadataDBConstants.INSERTQUERY
									.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
									.replace("{$columns}",
											"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,daily_flag,job_schedule_time,schedule_type,project_id")
									.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
											+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
											+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_materialize"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + SchedulerConstants.mat_script
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + target.split("~")[0]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
											+ schDto.getFeed_name() + "_write" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + hours + ":" + minutes + ":00"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + schDto.getProject_seq());

							statement.execute(insertMatJob);
						} else {
							jobNames[index] = schDto.getFeed_name() + "_" + target.split("~")[0] + "_write";
						}
						index++;
					}
					String insertDeleteJob = MetadataDBConstants.INSERTQUERY
							.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
							.replace("{$columns}",
									"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
											+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
											+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
											+ ",schedule_type,job_schedule_time,daily_flag,project_id")
							.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
									+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
									+ schDto.getFeed_name() + "_delete" + MetadataDBConstants.QUOTE
									+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name()
									+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
									+ schDto.getFeed_id() + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
									+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
									+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
									+ schDto.getFeed_name() + "~" + schDto.getCountry_code() + MetadataDBConstants.QUOTE
									+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
									+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
									+ jobNames[0] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
									+ MetadataDBConstants.QUOTE + jobNames[1] + MetadataDBConstants.QUOTE
									+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[2]
									+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
									+ jobNames[3] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
									+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
									+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
									+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
									+ jobNames[6] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
									+ MetadataDBConstants.QUOTE + jobNames[7] + MetadataDBConstants.QUOTE
									+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[8]
									+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
									+ jobNames[9] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
									+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
									+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours + ":" + minutes
									+ ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
									+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
									+ MetadataDBConstants.COMMA + schDto.getProject_seq());

					statement.executeQuery(insertDeleteJob);

				}
			}
		}
		if (monthlyFlag.equalsIgnoreCase("Y")) {
			if (dates.contains(",")) {
				for (String date : dates.split(",")) {
					if (hours.contains(",")) {
						for (String hour : hours.split(",")) {
							if (minutes.contains(",")) {
								for (String minute : minutes.split(",")) {
									insertQuery = MetadataDBConstants.INSERTQUERY
											.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
											.replace("{$columns}",
													"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
											.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ "_read" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
													+ schDto.getFeed_src_type() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + date
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + hour + ":" + minute + ":00"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE + "R"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.COMMA + schDto.getProject_seq());

									statement.execute(insertQuery);

									for (String target : tarArr) {

										String insertWriteJob = MetadataDBConstants.INSERTQUERY
												.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
												.replace("{$columns}",
														"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
												.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ "_" + target.split("~")[0] + "_write"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
														+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ schDto.getFeed_id() + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ SchedulerConstants.write_script + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ target.split("~")[0] + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ "$RUN_ID$" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + date + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour
														+ ":" + minute + ":00" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.COMMA
														+ schDto.getProject_seq());

										statement.execute(insertWriteJob);

										if (target.split("~")[1].equalsIgnoreCase("Y")) {

											jobNames[index] = schDto.getFeed_name() + "_materialize";

											String insertMatJob = MetadataDBConstants.INSERTQUERY
													.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
													.replace("{$columns}",
															"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
													.replace("{$data}", MetadataDBConstants.QUOTE
															+ schDto.getFeed_name() + "_" + target.split("~")[0]
															+ "_materialize" + MetadataDBConstants.QUOTE
															+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
															+ schDto.getFeed_name() + "_" + target.split("~")[0]
															+ "_materialize" + MetadataDBConstants.QUOTE
															+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
															+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
															+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
															+ schDto.getFeed_id() + MetadataDBConstants.QUOTE
															+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
															+ SchedulerConstants.mat_script + MetadataDBConstants.QUOTE
															+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
															+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
															+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
															+ target.split("~")[0] + MetadataDBConstants.QUOTE
															+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
															+ "$RUN_ID$" + MetadataDBConstants.QUOTE
															+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
															+ schDto.getFeed_name() + "_write"
															+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
															+ MetadataDBConstants.QUOTE + "Y"
															+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
															+ MetadataDBConstants.QUOTE + date
															+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
															+ MetadataDBConstants.QUOTE + hour + ":" + minute + ":00"
															+ MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE
															+ "R" + MetadataDBConstants.QUOTE
															+ MetadataDBConstants.COMMA + MetadataDBConstants.COMMA
															+ schDto.getProject_seq());

											statement.execute(insertMatJob);
										} else {
											jobNames[index] = schDto.getFeed_name() + "_" + target.split("~")[0]
													+ "_write";
										}
										index++;
									}
									String insertDeleteJob = MetadataDBConstants.INSERTQUERY
											.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
											.replace("{$columns}",
													"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
															+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
															+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
															+ ",schedule_type,job_schedule_time,monthly_flag,month_run_day,project_id")
											.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ "_delete" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
													+ schDto.getCountry_code() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[0]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[1]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[2]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[3]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[4]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[5]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[6]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[7]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[8]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[9]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour + ":"
													+ minute + ":00" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + date
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ schDto.getProject_seq());

									statement.executeQuery(insertDeleteJob);

								}

							} else {
								insertQuery = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
										.replace("{$data}",
												MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
														+ schDto.getFeed_src_type() + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ "$RUN_ID$" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ schDto.getExtraction_mode() + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + date + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + date
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + hour + ":" + minutes + ":00"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE + "R"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.COMMA + schDto.getProject_seq());

								statement.execute(insertQuery);

								for (String target : tarArr) {

									String insertWriteJob = MetadataDBConstants.INSERTQUERY
											.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
											.replace("{$columns}",
													"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
											.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
													+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + SchedulerConstants.write_script
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + target.split("~")[0]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + date + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour + ":"
													+ minutes + ":00" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.COMMA
													+ schDto.getProject_seq());

									statement.execute(insertWriteJob);

									if (target.split("~")[1].equalsIgnoreCase("Y")) {

										jobNames[index] = schDto.getFeed_name() + "_materialize";

										String insertMatJob = MetadataDBConstants.INSERTQUERY
												.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
												.replace("{$columns}",
														"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
												.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ "_" + target.split("~")[0] + "_materialize"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
														+ target.split("~")[0] + "_materialize"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + SchedulerConstants.mat_script
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + target.split("~")[0]
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "$RUN_ID$"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_write"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + date
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + hour + ":" + minutes + ":00"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE + "R"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.COMMA + schDto.getProject_seq());

										statement.execute(insertMatJob);
									} else {
										jobNames[index] = schDto.getFeed_name() + "_" + target.split("~")[0] + "_write";
									}
									index++;
								}

								String insertDeleteJob = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
														+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
														+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
														+ ",schedule_type,job_schedule_time,monthly_flag,month_run_day,project_id")
										.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ "_delete" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
												+ schDto.getCountry_code() + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[0] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[1]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[2] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[3]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[6] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[7]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[8] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[9]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour + ":"
												+ minutes + ":00" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + date
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ schDto.getProject_seq());

								statement.executeQuery(insertDeleteJob);

							}
						}

					} else {
						if (minutes.contains(",")) {
							for (String minute : minutes.split(",")) {
								insertQuery = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
										.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
												+ schDto.getFeed_src_type() + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + date
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + hours + ":" + minute + ":00"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE + "R"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.COMMA + schDto.getProject_seq());

								statement.execute(insertQuery);
								for (String target : tarArr) {

									String insertWriteJob = MetadataDBConstants.INSERTQUERY
											.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
											.replace("{$columns}",
													"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
											.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
													+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + SchedulerConstants.write_script
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + target.split("~")[0]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + date + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours
													+ ":" + minute + ":00" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.COMMA
													+ schDto.getProject_seq());

									statement.execute(insertWriteJob);

									if (target.split("~")[1].equalsIgnoreCase("Y")) {

										jobNames[index] = schDto.getFeed_name() + "_materialize";

										String insertMatJob = MetadataDBConstants.INSERTQUERY
												.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
												.replace("{$columns}",
														"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
												.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ "_" + target.split("~")[0] + "_materialize"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
														+ target.split("~")[0] + "_materialize"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + SchedulerConstants.mat_script
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + target.split("~")[0]
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "$RUN_ID$"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_write"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + date
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + hours + ":" + minute + ":00"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE + "R"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.COMMA + schDto.getProject_seq());

										statement.execute(insertMatJob);
									} else {
										jobNames[index] = schDto.getFeed_name() + "_" + target.split("~")[0] + "_write";
									}
									index++;
								}
								String insertDeleteJob = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
														+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
														+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
														+ ",schedule_type,job_schedule_time,monthly_flag,month_run_day,project_id")
										.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ "_delete" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
												+ schDto.getCountry_code() + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[0] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[1]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[2] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[3]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[6] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[7]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[8] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[9]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours + ":"
												+ minute + ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE
												+ "Y" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + date + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + schDto.getProject_seq());

								statement.executeQuery(insertDeleteJob);
							}

						} else {
							insertQuery = MetadataDBConstants.INSERTQUERY
									.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
									.replace("{$columns}",
											"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
									.replace("{$data}",
											MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
													+ schDto.getFeed_src_type() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + date
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + hours + ":" + minutes + ":00"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE + "R"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.COMMA + schDto.getProject_seq());

							statement.execute(insertQuery);
							for (String target : tarArr) {

								String insertWriteJob = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
										.replace("{$data}",
												MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
														+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + SchedulerConstants.write_script
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + target.split("~")[0]
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "$RUN_ID$"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + date
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + hours + ":" + minutes + ":00"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE + "R"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.COMMA + schDto.getProject_seq());

								statement.execute(insertWriteJob);

								if (target.split("~")[1].equalsIgnoreCase("Y")) {

									jobNames[index] = schDto.getFeed_name() + "_materialize";
									String insertMatJob = MetadataDBConstants.INSERTQUERY
											.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
											.replace("{$columns}",
													"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
											.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
													+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_" + target.split("~")[0]
													+ "_materialize" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_id() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ SchedulerConstants.mat_script + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ target.split("~")[0] + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_write"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + date
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + hours + ":" + minutes + ":00"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE + "R"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.COMMA + schDto.getProject_seq());

									statement.execute(insertMatJob);
								} else {
									jobNames[index] = schDto.getFeed_name() + "_write";
								}
								index++;
							}
							String insertDeleteJob = MetadataDBConstants.INSERTQUERY
									.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
									.replace("{$columns}",
											"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
													+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
													+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
													+ ",schedule_type,job_schedule_time,monthly_flag,month_run_day,project_id")
									.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
											+ schDto.getCountry_code() + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[0] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[1]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[2] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[3]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[6] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[7]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[8] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[9]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours + ":"
											+ minutes + ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE
											+ "Y" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + date + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + schDto.getProject_seq());

							statement.executeQuery(insertDeleteJob);
						}
					}
				}
			} else {
				if (hours.contains(",")) {
					for (String hour : hours.split(",")) {
						if (minutes.contains(",")) {
							for (String minute : minutes.split(",")) {
								insertQuery = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
										.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
												+ schDto.getFeed_src_type() + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + dates
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + hour + ":" + minute + ":00"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE + "R"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.COMMA + schDto.getProject_seq());

								statement.execute(insertQuery);

								for (String target : tarArr) {

									String insertWriteJob = MetadataDBConstants.INSERTQUERY
											.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
											.replace("{$columns}",
													"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
											.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
													+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + SchedulerConstants.write_script
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + target.split("~")[0]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + dates + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour + ":"
													+ minute + ":00" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.COMMA
													+ schDto.getProject_seq());

									statement.execute(insertWriteJob);

									if (target.split("~")[1].equalsIgnoreCase("Y")) {

										jobNames[index] = schDto.getFeed_name() + "_materialize";
										String insertMatJob = MetadataDBConstants.INSERTQUERY
												.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
												.replace("{$columns}",
														"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
												.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ "_" + target.split("~")[0] + "_materialize"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
														+ target.split("~")[0] + "_materialize"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + SchedulerConstants.mat_script
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + target.split("~")[0]
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "$RUN_ID$"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_write"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + dates
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + hour + ":" + minute + ":00"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE + "R"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.COMMA + schDto.getProject_seq());

										statement.execute(insertMatJob);
									} else {
										jobNames[index] = schDto.getFeed_name() + "_write";
									}
									index++;
								}
								String insertDeleteJob = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
														+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
														+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
														+ ",schedule_type,job_schedule_time,monthly_flag,month_run_day,project_id")
										.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ "_delete" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
												+ schDto.getCountry_code() + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[0] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[1]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[2] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[3]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[6] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[7]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[8] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[9]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour + ":"
												+ minute + ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE
												+ "Y" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + dates + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + schDto.getProject_seq());

								statement.executeQuery(insertDeleteJob);
							}

						} else {
							insertQuery = MetadataDBConstants.INSERTQUERY
									.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
									.replace("{$columns}",
											"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
									.replace("{$data}",
											MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
													+ schDto.getFeed_src_type() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + dates
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + hour + ":" + minutes + ":00"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE + "R"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.COMMA + schDto.getProject_seq());

							statement.execute(insertQuery);
							for (String target : tarArr) {

								String insertWriteJob = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
										.replace("{$data}",
												MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
														+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + SchedulerConstants.write_script
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + target.split("~")[0]
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "$RUN_ID$"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + dates
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + hour + ":" + minutes + ":00"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE + "R"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.COMMA + schDto.getProject_seq());

								statement.execute(insertWriteJob);

								if (target.split("~")[1].equalsIgnoreCase("Y")) {

									jobNames[index] = schDto.getFeed_name() + "_materialize";
									String insertMatJob = MetadataDBConstants.INSERTQUERY
											.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
											.replace("{$columns}",
													"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
											.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
													+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_" + target.split("~")[0]
													+ "_materialize" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_id() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ SchedulerConstants.mat_script + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ target.split("~")[0] + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_write"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + dates
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + hour + ":" + minutes + ":00"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE + "R"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.COMMA + schDto.getProject_seq());

									statement.execute(insertMatJob);
								} else {
									jobNames[index] = schDto.getFeed_name() + "_write";
								}
								index++;
							}
							String insertDeleteJob = MetadataDBConstants.INSERTQUERY
									.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
									.replace("{$columns}",
											"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
													+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
													+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
													+ ",schedule_type,job_schedule_time,monthly_flag,month_run_day,project_id")
									.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
											+ schDto.getCountry_code() + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[0] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[1]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[2] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[3]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[6] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[7]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[8] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[9]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour + ":"
											+ minutes + ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE
											+ "Y" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + dates + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + schDto.getProject_seq());

							statement.executeQuery(insertDeleteJob);
						}
					}

				} else {
					if (minutes.contains(",")) {
						for (String minute : minutes.split(",")) {
							insertQuery = MetadataDBConstants.INSERTQUERY
									.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
									.replace("{$columns}",
											"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
									.replace("{$data}",
											MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
													+ schDto.getFeed_src_type() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + dates
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + hours + ":" + minute + ":00"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE + "R"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.COMMA + schDto.getProject_seq());

							statement.execute(insertQuery);
							for (String target : tarArr) {

								String insertWriteJob = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
										.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
												+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
												+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + SchedulerConstants.write_script
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + target.split("~")[0]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
												+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + dates + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours + ":"
												+ minute + ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE
												+ "R" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.COMMA + schDto.getProject_seq());

								statement.execute(insertWriteJob);

								if (target.split("~")[1].equalsIgnoreCase("Y")) {

									jobNames[index] = schDto.getFeed_name() + "_materialize";
									String insertMatJob = MetadataDBConstants.INSERTQUERY
											.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
											.replace("{$columns}",
													"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
											.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
													+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_" + target.split("~")[0]
													+ "_materialize" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_id() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ SchedulerConstants.mat_script + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ target.split("~")[0] + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_write"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + dates
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + hours + ":" + minute + ":00"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE + "R"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.COMMA + schDto.getProject_seq());

									statement.execute(insertMatJob);
								} else {
									jobNames[index] = schDto.getFeed_name() + "_write";
								}
								index++;
							}
							String insertDeleteJob = MetadataDBConstants.INSERTQUERY
									.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
									.replace("{$columns}",
											"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
													+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
													+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
													+ ",schedule_type,job_schedule_time,monthly_flag,month_run_day,project_id")
									.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
											+ schDto.getCountry_code() + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[0] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[1]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[2] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[3]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[6] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[7]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[8] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[9]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours + ":"
											+ minute + ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.QUOTE
											+ "Y" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + dates + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + schDto.getProject_seq());

							statement.executeQuery(insertDeleteJob);
						}

					} else {
						insertQuery = MetadataDBConstants.INSERTQUERY
								.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
								.replace("{$columns}",
										"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
								.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_id()
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
										+ schDto.getFeed_src_type() + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + dates
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + hours + ":" + minutes + ":00"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + schDto.getProject_seq());

						statement.execute(insertQuery);
						for (String target : tarArr) {

							String insertWriteJob = MetadataDBConstants.INSERTQUERY
									.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
									.replace("{$columns}",
											"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
									.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
											+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
											+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + SchedulerConstants.write_script
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + target.split("~")[0]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
											+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + dates + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours + ":"
											+ minutes + ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + schDto.getProject_seq());

							statement.execute(insertWriteJob);

							if (target.split("~")[1].equalsIgnoreCase("Y")) {

								jobNames[index] = schDto.getFeed_name() + "_materialize";
								String insertMatJob = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,monthly_flag,month_run_day,job_schedule_time,schedule_type,project_id")
										.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
												+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
												+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_materialize"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + SchedulerConstants.mat_script
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + target.split("~")[0]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
												+ schDto.getFeed_name() + "_write" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + dates + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours + ":"
												+ minutes + ":00" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "R"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ schDto.getProject_seq());

								statement.execute(insertMatJob);
							} else {
								jobNames[index] = schDto.getFeed_name() + "_write";
							}
							index++;
						}
						String insertDeleteJob = MetadataDBConstants.INSERTQUERY
								.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
								.replace("{$columns}",
										"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
												+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
												+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
												+ ",schedule_type,job_schedule_time,monthly_flag,month_run_day,project_id")
								.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_id()
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
										+ schDto.getCountry_code() + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[0] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[1]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[2] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[3]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[6] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[7]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[8] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[9]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours + ":" + minutes
										+ ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + dates
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ schDto.getProject_seq());

						statement.executeQuery(insertDeleteJob);
					}
				}
			}
		}
		if (weeklyFlag.equalsIgnoreCase("Y")) {
			if (daysOfWeek.contains(",")) {
				for (String day : daysOfWeek.split(",")) {
					if (hours.contains(",")) {
						for (String hour : hours.split(",")) {
							if (minutes.contains(",")) {
								for (String minute : minutes.split(",")) {
									insertQuery = MetadataDBConstants.INSERTQUERY
											.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
											.replace("{$columns}",
													"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
											.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ "_read" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
													+ schDto.getFeed_src_type() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + day
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + hour + ":" + minute + ":00"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + schDto.getProject_seq());
									statement.execute(insertQuery);

									for (String target : tarArr) {

										String insertWriteJob = MetadataDBConstants.INSERTQUERY
												.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
												.replace("{$columns}",
														"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
												.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ "_" + target.split("~")[0] + "_write"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
														+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ schDto.getFeed_id() + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ SchedulerConstants.write_script + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ target.split("~")[0] + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ "$RUN_ID$" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + day + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour
														+ ":" + minute + ":00" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "R"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ schDto.getProject_seq());

										statement.execute(insertWriteJob);

										if (target.split("~")[1].equalsIgnoreCase("Y")) {

											jobNames[index] = schDto.getFeed_name() + "_materialize";
											String insertMatJob = MetadataDBConstants.INSERTQUERY
													.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
													.replace("{$columns}",
															"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
													.replace("{$data}", MetadataDBConstants.QUOTE
															+ schDto.getFeed_name() + "_" + target.split("~")[0]
															+ "_materialize" + MetadataDBConstants.QUOTE
															+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
															+ schDto.getFeed_name() + "_" + target.split("~")[0]
															+ "_materialize" + MetadataDBConstants.QUOTE
															+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
															+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
															+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
															+ schDto.getFeed_id() + MetadataDBConstants.QUOTE
															+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
															+ SchedulerConstants.mat_script + MetadataDBConstants.QUOTE
															+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
															+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
															+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
															+ target.split("~")[0] + MetadataDBConstants.QUOTE
															+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
															+ "$RUN_ID$" + MetadataDBConstants.QUOTE
															+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
															+ schDto.getFeed_name() + "_write"
															+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
															+ MetadataDBConstants.QUOTE + "Y"
															+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
															+ MetadataDBConstants.QUOTE + day
															+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
															+ MetadataDBConstants.QUOTE + hour + ":" + minute + ":00"
															+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
															+ MetadataDBConstants.QUOTE + "R"
															+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
															+ schDto.getProject_seq());

											statement.execute(insertMatJob);
										} else {
											jobNames[index] = schDto.getFeed_name() + "_write";
										}
										index++;
									}

									String insertDeleteJob = MetadataDBConstants.INSERTQUERY
											.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
											.replace("{$columns}",
													"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
															+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
															+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
															+ ",schedule_type,job_schedule_time,weekly_flag,week_run_day,project_id")
											.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ "_delete" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
													+ schDto.getCountry_code() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[0]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[1]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[2]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[3]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[4]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[5]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[6]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[7]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[8]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + jobNames[9]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour + ":"
													+ minute + ":00" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + day + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + schDto.getProject_seq());

									statement.executeQuery(insertDeleteJob);
								}
							} else {
								insertQuery = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
										.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
												+ schDto.getFeed_src_type() + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + day
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + hour + ":" + minutes + ":00"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + schDto.getProject_seq());
								statement.execute(insertQuery);
								for (String target : tarArr) {

									String insertWriteJob = MetadataDBConstants.INSERTQUERY
											.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
											.replace("{$columns}",
													"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
											.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
													+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + SchedulerConstants.write_script
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + target.split("~")[0]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + day + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour + ":"
													+ minutes + ":00" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "R"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ schDto.getProject_seq());

									statement.execute(insertWriteJob);

									if (target.split("~")[1].equalsIgnoreCase("Y")) {

										jobNames[index] = schDto.getFeed_name() + "_materialize";
										String insertMatJob = MetadataDBConstants.INSERTQUERY
												.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
												.replace("{$columns}",
														"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
												.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ "_" + target.split("~")[0] + "_materialize"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
														+ target.split("~")[0] + "_materialize"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + SchedulerConstants.mat_script
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + target.split("~")[0]
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "$RUN_ID$"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_write"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + day
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + hour + ":" + minutes + ":00"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + schDto.getProject_seq());

										statement.execute(insertMatJob);
									} else {
										jobNames[index] = schDto.getFeed_name() + "_write";
									}
									index++;
								}
								String insertDeleteJob = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
														+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
														+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
														+ ",schedule_type,job_schedule_time,weekly_flag,week_run_day,project_id")
										.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ "_delete" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
												+ schDto.getCountry_code() + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[0] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[1]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[2] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[3]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[6] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[7]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[8] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[9]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour + ":"
												+ minutes + ":00" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + day + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + schDto.getProject_seq());

								statement.executeQuery(insertDeleteJob);
							}
						}
					} else {

						if (minutes.contains(",")) {
							for (String minute : minutes.split(",")) {
								insertQuery = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
										.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
												+ schDto.getFeed_src_type() + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + day
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + hours + ":" + minute + ":00"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + schDto.getProject_seq());
								statement.execute(insertQuery);
								for (String target : tarArr) {

									String insertWriteJob = MetadataDBConstants.INSERTQUERY
											.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
											.replace("{$columns}",
													"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
											.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
													+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()

													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + SchedulerConstants.write_script
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + target.split("~")[0]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + day + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours
													+ ":" + minute + ":00" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "R"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ schDto.getProject_seq());

									statement.execute(insertWriteJob);

									if (target.split("~")[1].equalsIgnoreCase("Y")) {

										jobNames[index] = schDto.getFeed_name() + "_materialize";
										String insertMatJob = MetadataDBConstants.INSERTQUERY
												.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
												.replace("{$columns}",
														"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
												.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ "_" + target.split("~")[0] + "_materialize"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
														+ target.split("~")[0] + "_materialize"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + SchedulerConstants.mat_script
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + target.split("~")[0]
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "$RUN_ID$"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_write"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + day
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + hours + ":" + minute + ":00"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + schDto.getProject_seq());

										statement.execute(insertMatJob);
									} else {
										jobNames[index] = schDto.getFeed_name() + "_write";
									}
									index++;
								}
								String insertDeleteJob = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
														+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
														+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
														+ ",schedule_type,job_schedule_time,weekly_flag,week_run_day,project_id")
										.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ "_delete" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
												+ schDto.getCountry_code() + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[0] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[1]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[2] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[3]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[6] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[7]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[8] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[9]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours + ":"
												+ minute + ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + day
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ schDto.getProject_seq());

								statement.executeQuery(insertDeleteJob);
							}
						} else {
							insertQuery = MetadataDBConstants.INSERTQUERY
									.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
									.replace("{$columns}",
											"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
									.replace("{$data}",
											MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
													+ schDto.getFeed_src_type() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + day
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + hours + ":" + minutes + ":00"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + schDto.getProject_seq());
							statement.execute(insertQuery);
							for (String target : tarArr) {

								String insertWriteJob = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
										.replace("{$data}",
												MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
														+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + SchedulerConstants.write_script
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + target.split("~")[0]
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "$RUN_ID$"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + day
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + hours + ":" + minutes + ":00"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + schDto.getProject_seq());

								statement.execute(insertWriteJob);

								if (target.split("~")[1].equalsIgnoreCase("Y")) {

									jobNames[index] = schDto.getFeed_name() + "_materialize";
									String insertMatJob = MetadataDBConstants.INSERTQUERY
											.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
											.replace("{$columns}",
													"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
											.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
													+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_" + target.split("~")[0]
													+ "_materialize" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_id() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ SchedulerConstants.mat_script + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ target.split("~")[0] + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_write"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + day
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + hours + ":" + minutes + ":00"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + schDto.getProject_seq());

									statement.execute(insertMatJob);
								} else {
									jobNames[index] = schDto.getFeed_name() + "_write";
								}
								index++;
							}
							String insertDeleteJob = MetadataDBConstants.INSERTQUERY
									.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
									.replace("{$columns}",
											"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
													+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
													+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
													+ ",schedule_type,job_schedule_time,weekly_flag,week_run_day,project_id")
									.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
											+ schDto.getCountry_code() + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[0] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[1]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[2] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[3]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[6] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[7]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[8] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[9]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours + ":"
											+ minutes + ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + day
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ schDto.getProject_seq());

							statement.executeQuery(insertDeleteJob);
						}
					}
				}
			} else {
				if (hours.contains(",")) {
					for (String hour : hours.split(",")) {
						if (minutes.contains(",")) {
							for (String minute : minutes.split(",")) {
								insertQuery = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
										.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
												+ schDto.getFeed_src_type() + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + daysOfWeek
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + hour + ":" + minute + ":00"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + schDto.getProject_seq());
								statement.execute(insertQuery);
								for (String target : tarArr) {

									String insertWriteJob = MetadataDBConstants.INSERTQUERY
											.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
											.replace("{$columns}",
													"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
											.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
													+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + SchedulerConstants.write_script
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + target.split("~")[0]
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + daysOfWeek + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour + ":"
													+ minute + ":00" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "R"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ schDto.getProject_seq());

									statement.execute(insertWriteJob);

									if (target.split("~")[1].equalsIgnoreCase("Y")) {

										jobNames[index] = schDto.getFeed_name() + "_materialize";
										String insertMatJob = MetadataDBConstants.INSERTQUERY
												.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
												.replace("{$columns}",
														"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
												.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ "_" + target.split("~")[0] + "_materialize"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
														+ target.split("~")[0] + "_materialize"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + SchedulerConstants.mat_script
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + target.split("~")[0]
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "$RUN_ID$"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_write"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
														+ daysOfWeek + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour
														+ ":" + minute + ":00" + MetadataDBConstants.QUOTE
														+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "R"
														+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
														+ schDto.getProject_seq());

										statement.execute(insertMatJob);
									} else {
										jobNames[index] = schDto.getFeed_name() + "_write";
									}
									index++;
								}
								String insertDeleteJob = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
														+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
														+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
														+ ",schedule_type,job_schedule_time,weekly_flag,week_run_day,project_id")
										.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ "_delete" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
												+ schDto.getCountry_code() + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[0] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[1]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[2] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[3]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[6] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[7]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + jobNames[8] + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[9]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour + ":"
												+ minute + ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + daysOfWeek
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ schDto.getProject_seq());

								statement.executeQuery(insertDeleteJob);
							}
						} else {
							insertQuery = MetadataDBConstants.INSERTQUERY
									.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
									.replace("{$columns}",
											"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
									.replace("{$data}",
											MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
													+ schDto.getFeed_src_type() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + daysOfWeek
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + hour + ":" + minutes + ":00"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + schDto.getProject_seq());
							statement.execute(insertQuery);
							for (String target : tarArr) {

								String insertWriteJob = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
										.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
												+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
												+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + SchedulerConstants.write_script
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + target.split("~")[0]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
												+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + daysOfWeek + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour + ":"
												+ minutes + ":00" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "R"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ schDto.getProject_seq());

								statement.execute(insertWriteJob);

								if (target.split("~")[1].equalsIgnoreCase("Y")) {

									jobNames[index] = schDto.getFeed_name() + "_materialize";
									String insertMatJob = MetadataDBConstants.INSERTQUERY
											.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
											.replace("{$columns}",
													"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
											.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
													+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_" + target.split("~")[0]
													+ "_materialize" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_id() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ SchedulerConstants.mat_script + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ target.split("~")[0] + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_write"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + daysOfWeek
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + hour + ":" + minutes + ":00"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + schDto.getProject_seq());

									statement.execute(insertMatJob);
								} else {
									jobNames[index] = schDto.getFeed_name() + "_write";
								}
								index++;
							}
							String insertDeleteJob = MetadataDBConstants.INSERTQUERY
									.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
									.replace("{$columns}",
											"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
													+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
													+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
													+ ",schedule_type,job_schedule_time,weekly_flag,week_run_day,project_id")
									.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
											+ schDto.getCountry_code() + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[0] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[1]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[2] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[3]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[6] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[7]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[8] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[9]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hour + ":"
											+ minutes + ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + daysOfWeek
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ schDto.getProject_seq());

							statement.executeQuery(insertDeleteJob);
						}
					}
				} else {

					if (minutes.contains(",")) {
						for (String minute : minutes.split(",")) {
							insertQuery = MetadataDBConstants.INSERTQUERY
									.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
									.replace("{$columns}",
											"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
									.replace("{$data}",
											MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
													+ schDto.getFeed_src_type() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + daysOfWeek
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + hours + ":" + minute + ":00"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + schDto.getProject_seq());
							statement.execute(insertQuery);
							for (String target : tarArr) {

								String insertWriteJob = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
										.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
												+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
												+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + SchedulerConstants.write_script
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + target.split("~")[0]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
												+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + daysOfWeek + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours + ":"
												+ minute + ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + schDto.getProject_seq());

								statement.execute(insertWriteJob);

								if (target.split("~")[1].equalsIgnoreCase("Y")) {
									jobNames[index] = schDto.getFeed_name() + "_materialize";
									String insertMatJob = MetadataDBConstants.INSERTQUERY
											.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
											.replace("{$columns}",
													"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
											.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
													+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + "_" + target.split("~")[0]
													+ "_materialize" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_id() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ SchedulerConstants.mat_script + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ schDto.getFeed_name() + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
													+ target.split("~")[0] + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_write"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + daysOfWeek
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + hours + ":" + minute + ":00"
													+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
													+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
													+ MetadataDBConstants.COMMA + schDto.getProject_seq());

									statement.execute(insertMatJob);
								} else {
									jobNames[index] = schDto.getFeed_name() + "_write";
								}
								index++;
							}
							String insertDeleteJob = MetadataDBConstants.INSERTQUERY
									.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
									.replace("{$columns}",
											"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
													+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
													+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
													+ ",schedule_type,job_schedule_time,weekly_flag,week_run_day,project_id")
									.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
											+ schDto.getCountry_code() + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[0] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[1]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[2] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[3]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[6] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[7]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + jobNames[8] + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[9]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours + ":"
											+ minute + ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + daysOfWeek
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ schDto.getProject_seq());

							statement.executeQuery(insertDeleteJob);
						}
					} else {
						insertQuery = MetadataDBConstants.INSERTQUERY
								.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
								.replace("{$columns}",
										"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_5,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
								.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_id()
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + SchedulerConstants.read_script
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
										+ schDto.getFeed_src_type() + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + daysOfWeek
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + hours + ":" + minutes + ":00"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + schDto.getProject_seq());
						statement.execute(insertQuery);
						for (String target : tarArr) {

							String insertWriteJob = MetadataDBConstants.INSERTQUERY
									.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
									.replace("{$columns}",
											"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
									.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
											+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
											+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + SchedulerConstants.write_script
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + target.split("~")[0]
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
											+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
											+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + daysOfWeek + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours + ":"
											+ minutes + ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
											+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
											+ MetadataDBConstants.COMMA + schDto.getProject_seq());

							statement.execute(insertWriteJob);

							if (target.split("~")[1].equalsIgnoreCase("Y")) {

								jobNames[index] = schDto.getFeed_name() + "_materialize";
								String insertMatJob = MetadataDBConstants.INSERTQUERY
										.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
										.replace("{$columns}",
												"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,weekly_flag,week_run_day,job_schedule_time,schedule_type,project_id")
										.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
												+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
												+ schDto.getFeed_name() + "_" + target.split("~")[0] + "_materialize"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_id()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + SchedulerConstants.mat_script
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + schDto.getFeed_name()
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + target.split("~")[0]
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
												+ schDto.getFeed_name() + "_write" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ MetadataDBConstants.QUOTE + daysOfWeek + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours + ":"
												+ minutes + ":00" + MetadataDBConstants.QUOTE
												+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "R"
												+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
												+ schDto.getProject_seq());

								statement.execute(insertMatJob);
							} else {
								jobNames[index] = schDto.getFeed_name() + "_write";
							}
							index++;
						}
						String insertDeleteJob = MetadataDBConstants.INSERTQUERY
								.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
								.replace("{$columns}",
										"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
												+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
												+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
												+ ",schedule_type,job_schedule_time,weekly_flag,week_run_day,project_id")
								.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_id()
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + SchedulerConstants.delete_script
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~"
										+ schDto.getCountry_code() + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[0] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[1]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[2] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[3]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[6] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[7]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + jobNames[8] + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[9]
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + "R" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + hours + ":" + minutes
										+ ":00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
										+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + daysOfWeek
										+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
										+ schDto.getProject_seq());

						statement.executeQuery(insertDeleteJob);
					}
				}
			}
		}

		if (hourlyFlag.equalsIgnoreCase("Y")) {
		}

		return "success";
	}

	/**
	 * @param conn
	 * @param schDto
	 * @param tarArr
	 * @return String
	 * @throws Exception
	 */
	private String scheduleEventBasedFeedFile(Connection conn, ScheduleExtractDto schDto, ArrayList<String> tarArr)
			throws Exception {

		int index = 0;
		String[] jobNames = new String[10];
		Arrays.fill(jobNames, "");

		String insertReadJob = MetadataDBConstants.INSERTQUERY.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
				.replace("{$columns}",
						"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_4,argument_5,schedule_type,job_schedule_time,daily_flag,project_id")
				.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_id()
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ SchedulerConstants.read_script + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~" + schDto.getFeed_src_type()
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ schDto.getFile_path() + "/" + schDto.getFile_pattern() + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getExtraction_mode()
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "F"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "00:00:00"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + schDto.getProject_seq());
		try {
			Statement statement = conn.createStatement();
			statement.executeQuery(insertReadJob);

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("Exception occured while scheduling Read job");
		}

		for (String target : tarArr) {

			String insertWriteJob = MetadataDBConstants.INSERTQUERY
					.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
					.replace("{$columns}",
							"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,schedule_type,job_schedule_time,daily_flag,project_id")
					.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_" + target.split("~")[0]
							+ "_write" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
							+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
							+ schDto.getFeed_name() + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + schDto.getFeed_id() + MetadataDBConstants.QUOTE
							+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + SchedulerConstants.write_script
							+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
							+ schDto.getFeed_name() + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + target.split("~")[0] + MetadataDBConstants.QUOTE
							+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
							+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
							+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + "F" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + "00:00:00" + MetadataDBConstants.QUOTE
							+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
							+ MetadataDBConstants.COMMA + schDto.getProject_seq());

			try {
				Statement statement = conn.createStatement();
				statement.executeQuery(insertWriteJob);

			} catch (Exception e) {
				e.printStackTrace();
				throw new Exception("Exception occured while scheduling write job");
			}
			if (target.split("~")[1].equalsIgnoreCase("Y")) {

				jobNames[index] = schDto.getFeed_name() + "_" + target.split("~")[0] + "_materialize";

				String insertMatJob = MetadataDBConstants.INSERTQUERY
						.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
						.replace("{$columns}",
								"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,schedule_type,job_schedule_time,daily_flag,project_id")
						.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
								+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
								+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name()
								+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
								+ schDto.getFeed_id() + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
								+ MetadataDBConstants.QUOTE + SchedulerConstants.mat_script + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name()
								+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
								+ target.split("~")[0] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
								+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
								+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "F"
								+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
								+ "00:00:00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
								+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + schDto.getProject_seq());
				try {
					Statement statement = conn.createStatement();
					statement.executeQuery(insertMatJob);
				} catch (Exception e) {
					e.printStackTrace();
					throw new Exception("Exception occured while scheduling materialization job");
				}
			} else {
				jobNames[index] = schDto.getFeed_name() + "_" + target.split("~")[0] + "_write";
			}
			index++;

		}
		String insertDeleteJob = MetadataDBConstants.INSERTQUERY.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
				.replace("{$columns}",
						"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
								+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
								+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
								+ ",schedule_type,job_schedule_time,daily_flag,project_id")
				.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ schDto.getFeed_name() + "_delete" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_id()
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ SchedulerConstants.delete_script + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~" + schDto.getCountry_code()
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ jobNames[0] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + jobNames[1] + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[2]
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ jobNames[3] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ jobNames[6] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + jobNames[7] + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[8]
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ jobNames[9] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + "F" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + "00:00:00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ schDto.getProject_seq());

		Statement statement = conn.createStatement();
		statement.executeQuery(insertDeleteJob);

		return "success";
	}

	/**
	 * @param conn
	 * @param schDto
	 * @param tarArr
	 * @return String
	 * @throws Exception
	 */
	private String scheduleEventBasedFeedKafka(Connection conn, ScheduleExtractDto schDto, ArrayList<String> tarArr)
			throws Exception {

		int index = 0;
		String[] jobNames = new String[10];
		Arrays.fill(jobNames, "");

		String insertReadJob = MetadataDBConstants.INSERTQUERY.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
				.replace("{$columns}",
						"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_4,argument_5,schedule_type,job_schedule_time,daily_flag,project_id")
				.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_id()
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ SchedulerConstants.read_script + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~" + schDto.getFeed_src_type()
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ schDto.getKafka_topic() + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode() + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "K" + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "00:00:00" + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + schDto.getProject_seq());
		try {
			Statement statement = conn.createStatement();
			statement.executeQuery(insertReadJob);

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("Exception occured while scheduling Read job");
		}

		for (String target : tarArr) {

			String insertWriteJob = MetadataDBConstants.INSERTQUERY
					.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
					.replace("{$columns}",
							"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,schedule_type,job_schedule_time,daily_flag,project_id")
					.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_" + target.split("~")[0]
							+ "_write" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
							+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
							+ schDto.getFeed_name() + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + schDto.getFeed_id() + MetadataDBConstants.QUOTE
							+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + SchedulerConstants.write_script
							+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
							+ schDto.getFeed_name() + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + target.split("~")[0] + MetadataDBConstants.QUOTE
							+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
							+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
							+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + "K" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + "00:00:00" + MetadataDBConstants.QUOTE
							+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
							+ MetadataDBConstants.COMMA + schDto.getProject_seq());

			try {
				Statement statement = conn.createStatement();
				statement.executeQuery(insertWriteJob);

			} catch (Exception e) {
				e.printStackTrace();
				throw new Exception("Exception occured while scheduling write job");
			}
			if (target.split("~")[1].equalsIgnoreCase("Y")) {

				jobNames[index] = schDto.getFeed_name() + "_" + target.split("~")[0] + "_materialize";
				String insertMatJob = MetadataDBConstants.INSERTQUERY
						.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
						.replace("{$columns}",
								"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,schedule_type,job_schedule_time,daily_flag,project_id")
						.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
								+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
								+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name()
								+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
								+ schDto.getFeed_id() + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
								+ MetadataDBConstants.QUOTE + SchedulerConstants.mat_script + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name()
								+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
								+ target.split("~")[0] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
								+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
								+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "K"
								+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
								+ "00:00:00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
								+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + schDto.getProject_seq());
				try {
					Statement statement = conn.createStatement();
					statement.executeQuery(insertMatJob);
				} catch (Exception e) {
					e.printStackTrace();
					throw new Exception("Exception occured while scheduling materialization job");
				}
			} else {
				jobNames[index] = schDto.getFeed_name() + "_" + target.split("~")[0] + "_write";
			}
			index++;
		}

		String insertDeleteJob = MetadataDBConstants.INSERTQUERY.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
				.replace("{$columns}",
						"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
								+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
								+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
								+ ",schedule_type,job_schedule_time,daily_flag,project_id")
				.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ schDto.getFeed_name() + "_delete" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_id()
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ SchedulerConstants.delete_script + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~" + schDto.getCountry_code()
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ jobNames[0] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + jobNames[1] + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[2]
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ jobNames[3] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ jobNames[6] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + jobNames[7] + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[8]
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ jobNames[9] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + "K" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + "00:00:00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ schDto.getProject_seq());

		Statement statement = conn.createStatement();
		statement.executeQuery(insertDeleteJob);

		return "success";
	}

	/**
	 * @param conn
	 * @param schDto
	 * @param tarArr
	 * @return String
	 * @throws Exception
	 */
	private String scheduleEventBasedFeedApi(Connection conn, ScheduleExtractDto schDto, ArrayList<String> tarArr)
			throws Exception {

		int index = 0;
		String[] jobNames = new String[10];
		Arrays.fill(jobNames, "");

		String insertReadJob = MetadataDBConstants.INSERTQUERY.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
				.replace("{$columns}",
						"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,argument_4,argument_5,schedule_type,job_schedule_time,daily_flag,project_id")
				.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_read"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_id()
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ SchedulerConstants.read_script + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~" + schDto.getFeed_src_type()
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ schDto.getApi_unique_key() + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + schDto.getExtraction_mode() + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "A" + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "00:00:00" + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + schDto.getProject_seq());
		try {
			Statement statement = conn.createStatement();
			statement.executeQuery(insertReadJob);

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("Exception occured while scheduling Read job");
		}

		for (String target : tarArr) {

			String insertWriteJob = MetadataDBConstants.INSERTQUERY
					.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
					.replace("{$columns}",
							"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,schedule_type,job_schedule_time,daily_flag,project_id")
					.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_" + target.split("~")[0]
							+ "_write" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_" + target.split("~")[0] + "_write"
							+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
							+ schDto.getFeed_name() + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + schDto.getFeed_id() + MetadataDBConstants.QUOTE
							+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + SchedulerConstants.write_script
							+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
							+ schDto.getFeed_name() + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + target.split("~")[0] + MetadataDBConstants.QUOTE
							+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
							+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
							+ schDto.getFeed_name() + "_read" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + "A" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
							+ MetadataDBConstants.QUOTE + "00:00:00" + MetadataDBConstants.QUOTE
							+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
							+ MetadataDBConstants.COMMA + schDto.getProject_seq());

			try {
				Statement statement = conn.createStatement();
				statement.executeQuery(insertWriteJob);

			} catch (Exception e) {
				e.printStackTrace();
				throw new Exception("Exception occured while scheduling write job");
			}
			if (target.split("~")[1].equalsIgnoreCase("Y")) {

				jobNames[index] = schDto.getFeed_name() + "_" + target.split("~")[0] + "_materialize";
				String insertMatJob = MetadataDBConstants.INSERTQUERY
						.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
						.replace("{$columns}",
								"job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,argument_3,PREDESSOR_JOB_ID_1,schedule_type,job_schedule_time,daily_flag,project_id")
						.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
								+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
								+ target.split("~")[0] + "_materialize" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name()
								+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
								+ schDto.getFeed_id() + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
								+ MetadataDBConstants.QUOTE + SchedulerConstants.mat_script + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name()
								+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
								+ target.split("~")[0] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
								+ MetadataDBConstants.QUOTE + "$RUN_ID$" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_"
								+ target.split("~")[0] + "_write" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "A"
								+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
								+ "00:00:00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
								+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE
								+ MetadataDBConstants.COMMA + schDto.getProject_seq());
				try {
					Statement statement = conn.createStatement();
					statement.executeQuery(insertMatJob);
				} catch (Exception e) {
					e.printStackTrace();
					throw new Exception("Exception occured while scheduling materialization job");
				}
			} else {
				jobNames[index] = schDto.getFeed_name() + "_" + target.split("~")[0] + "_write";
			}
			index++;
		}

		String insertDeleteJob = MetadataDBConstants.INSERTQUERY.replace("{$table}", MetadataDBConstants.SCHEDULETABLE)
				.replace("{$columns}",
						"job_id,job_name,batch_id,feed_id,command,argument_1,argument_3,PREDESSOR_JOB_ID_1,"
								+ "PREDESSOR_JOB_ID_2,PREDESSOR_JOB_ID_3,PREDESSOR_JOB_ID_4,PREDESSOR_JOB_ID_5,PREDESSOR_JOB_ID_6,"
								+ "PREDESSOR_JOB_ID_7,PREDESSOR_JOB_ID_8,PREDESSOR_JOB_ID_9,PREDESSOR_JOB_ID_10"
								+ ",schedule_type,job_schedule_time,daily_flag,project_id")
				.replace("{$data}", MetadataDBConstants.QUOTE + schDto.getFeed_name() + "_delete"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ schDto.getFeed_name() + "_delete" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + schDto.getFeed_id()
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ SchedulerConstants.delete_script + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + schDto.getFeed_name() + "~" + schDto.getCountry_code()
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "$RUN_ID$"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ jobNames[0] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + jobNames[1] + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[2]
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ jobNames[3] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + jobNames[4] + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[5]
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ jobNames[6] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + jobNames[7] + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + jobNames[8]
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ jobNames[9] + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + "A" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + "00:00:00" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + "Y" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ schDto.getProject_seq());

		Statement statement = conn.createStatement();
		statement.executeQuery(insertDeleteJob);

		return "success";
	}
}
