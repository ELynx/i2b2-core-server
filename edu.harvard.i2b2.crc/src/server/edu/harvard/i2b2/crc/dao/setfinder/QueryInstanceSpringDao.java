/*******************************************************************************
 * Copyright (c) 2006-2018 Massachusetts General Hospital 
 * All rights reserved. This program and the accompanying materials 
 * are made available under the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. I2b2 is also distributed under
 * the terms of the Healthcare Disclaimer.
 ******************************************************************************/
/*

 * 
 * Contributors: 
 *     Rajesh Kuttan
 */
package edu.harvard.i2b2.crc.dao.setfinder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.SqlUpdate;

import edu.harvard.i2b2.common.exception.I2B2DAOException;
import edu.harvard.i2b2.crc.dao.CRCDAO;
import edu.harvard.i2b2.crc.datavo.db.DataSourceLookup;
import edu.harvard.i2b2.crc.datavo.db.QtQueryInstance;
import edu.harvard.i2b2.crc.datavo.db.QtQueryMaster;
import edu.harvard.i2b2.crc.datavo.db.QtQueryStatusType;

/**
 * Class to handle persistance operation of Query instance i.e. each run of
 * query is called query instance $Id: QueryInstanceSpringDao.java,v 1.4
 * 2008/04/08 19:38:24 rk903 Exp $
 * 
 * @author rkuttan
 * @see QtQueryInstance
 */
public class QueryInstanceSpringDao extends CRCDAO implements IQueryInstanceDao {

	JdbcTemplate jdbcTemplate = null;
	SaveQueryInstance saveQueryInstance = null;
	QtQueryInstanceRowMapper queryInstanceMapper = null;
	private DataSourceLookup dataSourceLookup = null;
	
	/** log **/
	protected final Log log = LogFactory.getLog(getClass());

	public QueryInstanceSpringDao(DataSource dataSource,
			DataSourceLookup dataSourceLookup) {
		setDataSource(dataSource);
		setDbSchemaName(dataSourceLookup.getFullSchema());
		jdbcTemplate = new JdbcTemplate(dataSource);
		this.dataSourceLookup = dataSourceLookup;
		queryInstanceMapper = new QtQueryInstanceRowMapper();
	}

	/**
	 * Function to create query instance
	 * 
	 * @param queryMasterId
	 * @param userId
	 * @param groupId
	 * @param batchMode
	 * @param statusId
	 * @return query instance id
	 */
	@Override
	public String createQueryInstance(String queryMasterId, String userId,
									  String groupId, String batchMode, int statusId) {
		QtQueryInstance queryInstance = new QtQueryInstance();
		queryInstance.setUserId(userId);
		queryInstance.setGroupId(groupId);
		queryInstance.setBatchMode(batchMode);
		queryInstance.setDeleteFlag("N");

		QtQueryMaster queryMaster = new QtQueryMaster();
		queryMaster.setQueryMasterId(queryMasterId);
		queryInstance.setQtQueryMaster(queryMaster);

		QtQueryStatusType statusType = new QtQueryStatusType();
		statusType.setStatusTypeId(statusId);
		queryInstance.setQtQueryStatusType(statusType);

		Date startDate = new Date(System.currentTimeMillis());
		queryInstance.setStartDate(startDate);
		saveQueryInstance = new SaveQueryInstance(getDataSource(), getDbSchemaName(), dataSourceLookup);
		saveQueryInstance.save(queryInstance);
		return queryInstance.getQueryInstanceId();
	}

	/**
	 * Returns list of query instance for the given master id
	 * 
	 * @param queryMasterId
	 * @return List<QtQueryInstance>
	 */
	@Override
	@SuppressWarnings("unchecked")
	public List<QtQueryInstance> getQueryInstanceByMasterId(String queryMasterId) {
		String sql = "select *  from " + getDbSchemaName()
				+ "qt_query_instance where query_master_id = ?";
		List<QtQueryInstance> queryInstanceList = jdbcTemplate.query(sql,
				new Object[] { Integer.parseInt(queryMasterId) }, queryInstanceMapper);
		return queryInstanceList;
	}

	/**
	 * Find query instance by id
	 * 
	 * @param queryInstanceId
	 * @return QtQueryInstance
	 */
	@Override
	public QtQueryInstance getQueryInstanceByInstanceId(String queryInstanceId) {
		String sql = "select *  from " + getDbSchemaName()
				+ "qt_query_instance  where query_instance_id =?";

		QtQueryInstance queryInstance = (QtQueryInstance) jdbcTemplate
				.queryForObject(sql, new Object[] { Integer.parseInt(queryInstanceId ) },
						queryInstanceMapper);
		return queryInstance;
	}

	/**
	 * Update query instance
	 * 
	 * @param queryInstance
	 * @return QtQueryInstance
	 * @throws I2B2DAOException 
	 */
	@Override
	public QtQueryInstance update(QtQueryInstance queryInstance,
								  boolean appendMessageFlag) throws I2B2DAOException {
		Integer statusTypeId = (queryInstance.getQtQueryStatusType() != null)
				? queryInstance.getQtQueryStatusType().getStatusTypeId() : null;
		String messageUpdate;
		if (appendMessageFlag) {
			//String concatOperator = StringUtils.EMPTY;
			//concatOperator = "+";
			// messageUpdate = " MESSAGE = isnull(Cast(MESSAGE as nvarchar(4000)),'') "
			//		+ concatOperator + " ? ";
			// Cast(notes as nvarchar(4000))

			//update message field
			updateMessage(queryInstance.getQueryInstanceId(), queryInstance.getMessage(), true);

			if (queryInstance.getEndDate() != null) {
				//update rest of the fields
				String sql = "UPDATE " + getDbSchemaName()
						+ "QT_QUERY_INSTANCE set USER_ID = ?, GROUP_ID = ?,BATCH_MODE = ?,END_DATE = ? ,STATUS_TYPE_ID = ? "
						+ " where query_instance_id = ? ";

				jdbcTemplate.update(sql, new Object[] {
						queryInstance.getUserId(),
						queryInstance.getGroupId(),
						queryInstance.getBatchMode(),
						queryInstance.getEndDate(),
						statusTypeId,
						Integer.parseInt(queryInstance.getQueryInstanceId()) });
			} else {
				//update rest of the fields
				String sql = "UPDATE "
						+ getDbSchemaName()
						+ "QT_QUERY_INSTANCE set USER_ID = ?, GROUP_ID = ?,BATCH_MODE = ?,STATUS_TYPE_ID = ? "
						+ " where query_instance_id = ? ";

				jdbcTemplate.update(sql, new Object[] {
						queryInstance.getUserId(),
						queryInstance.getGroupId(),
						queryInstance.getBatchMode(),
						statusTypeId,
						Integer.parseInt(queryInstance.getQueryInstanceId()) });
			}
			return queryInstance;
		} else
			messageUpdate = " MESSAGE = ?";

		if (queryInstance.getEndDate() != null) {
			String sql = "UPDATE "
					+ getDbSchemaName()
					+ "QT_QUERY_INSTANCE set USER_ID = ?, GROUP_ID = ?,BATCH_MODE = ?,END_DATE = ? ,STATUS_TYPE_ID = ?, "
					+ messageUpdate + " where query_instance_id = ? ";

			jdbcTemplate.update(sql, new Object[] {
					queryInstance.getUserId(),
					queryInstance.getGroupId(),
					queryInstance.getBatchMode(),
					queryInstance.getEndDate(),
					statusTypeId,
					(queryInstance.getMessage() == null) ? StringUtils.EMPTY : queryInstance
							.getMessage(), Integer.parseInt(queryInstance.getQueryInstanceId())});
		} else {
			String sql = "UPDATE "
					+ getDbSchemaName()
					+ "QT_QUERY_INSTANCE set USER_ID = ?, GROUP_ID = ?,BATCH_MODE = ?,STATUS_TYPE_ID = ?, "
					+ messageUpdate + " where query_instance_id = ? ";

			jdbcTemplate.update(sql, new Object[]{
					queryInstance.getUserId(),
					queryInstance.getGroupId(),
					queryInstance.getBatchMode(),
					statusTypeId,
					(queryInstance.getMessage() == null) ? StringUtils.EMPTY : queryInstance
							.getMessage(), Integer.parseInt(queryInstance.getQueryInstanceId())});
		}
		return queryInstance;
	}

	/**
	 * Update query instance message
	 *
	 * @param queryInstanceId
	 * @param message
	 * @param appendMessageFlag
	 * @return
	 */
	@Override
	public void updateMessage(String queryInstanceId, String message,
							  boolean appendMessageFlag) throws I2B2DAOException {
		String messageUpdate = appendMessageFlag ? " MESSAGE = ? " : " MESSAGE = ?";
		String sql = "UPDATE " + getDbSchemaName()
				+ "QT_QUERY_INSTANCE set "
				+ messageUpdate + " where query_instance_id = ? ";
		jdbcTemplate.update(sql, new Object[] {
				(message == null) ? StringUtils.EMPTY : message, Integer.parseInt(queryInstanceId) });
	}
	
	private static class SaveQueryInstance extends SqlUpdate {
		private String INSERT_INTERSYSTEMS_IRIS;
		private String SEQUENCE_INTERSYSTEMS_IRIS;

		private DataSourceLookup dataSourceLookup;

		public SaveQueryInstance(DataSource dataSource, String dbSchemaName,
								 DataSourceLookup dataSourceLookup) {
			super();
			this.dataSourceLookup = dataSourceLookup;
			// sqlServerSequenceDao = new
			// SQLServerSequenceDAO(dataSource,dataSourceLookup) ;
			setDataSource(dataSource);
			INSERT_INTERSYSTEMS_IRIS = "INSERT INTO "
					+ dbSchemaName
					+ "QT_QUERY_INSTANCE "
					+ "(QUERY_INSTANCE_ID, QUERY_MASTER_ID, USER_ID, GROUP_ID,BATCH_MODE,START_DATE,END_DATE,STATUS_TYPE_ID,DELETE_FLAG) "
					+ "VALUES (?,?,?,?,?,?,?,?,?)";
			setSql(INSERT_INTERSYSTEMS_IRIS);
			SEQUENCE_INTERSYSTEMS_IRIS = "select I2B2.Utils_nextval('qt_query_instance_query_instance_id_seq')";
			declareParameter(new SqlParameter(Types.INTEGER));
			declareParameter(new SqlParameter(Types.INTEGER));
			declareParameter(new SqlParameter(Types.VARCHAR));
			declareParameter(new SqlParameter(Types.VARCHAR));
			declareParameter(new SqlParameter(Types.VARCHAR));
			declareParameter(new SqlParameter(Types.TIMESTAMP));
			declareParameter(new SqlParameter(Types.TIMESTAMP));
			declareParameter(new SqlParameter(Types.INTEGER));
			declareParameter(new SqlParameter(Types.VARCHAR));
			compile();
		}

		public void save(QtQueryInstance queryInstance) {
			JdbcTemplate jdbc = getJdbcTemplate();
			int queryInstanceId = 0;
			Object[] object = null;
			queryInstanceId = jdbc.queryForObject(SEQUENCE_INTERSYSTEMS_IRIS, Integer.class);
			queryInstance.setQueryInstanceId(String.valueOf(queryInstanceId));
			object = new Object[]{queryInstance.getQueryInstanceId(),
					queryInstance.getQtQueryMaster().getQueryMasterId(),
					queryInstance.getUserId(), queryInstance.getGroupId(),
					queryInstance.getBatchMode(),
					queryInstance.getStartDate(),
					queryInstance.getEndDate(),
					queryInstance.getQtQueryStatusType().getStatusTypeId(),
					queryInstance.getDeleteFlag()};
			update(object);
		}
	}

	private class QtQueryInstanceRowMapper implements RowMapper {
		QueryStatusTypeSpringDao statusTypeDao = new QueryStatusTypeSpringDao(dataSource, dataSourceLookup);

		@Override
		public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
			QtQueryInstance queryInstance = new QtQueryInstance();
			queryInstance.setQueryInstanceId(rs.getString("QUERY_INSTANCE_ID"));
			QtQueryMaster queryMaster = new QtQueryMaster();
			queryMaster.setQueryMasterId(rs.getString("QUERY_MASTER_ID"));
			queryInstance.setQtQueryMaster(queryMaster);
			queryInstance.setUserId(rs.getString("USER_ID"));
			queryInstance.setGroupId(rs.getString("GROUP_ID"));
			queryInstance.setBatchMode(rs.getString("BATCH_MODE"));
			queryInstance.setStartDate(rs.getTimestamp("START_DATE"));
			queryInstance.setEndDate(rs.getTimestamp("END_DATE"));
			queryInstance.setMessage(rs.getString("MESSAGE"));
			int statusTypeId = rs.getInt("STATUS_TYPE_ID");
			queryInstance.setQtQueryStatusType(statusTypeDao.getQueryStatusTypeById(statusTypeId));
			queryInstance.setDeleteFlag(rs.getString("DELETE_FLAG"));
			return queryInstance;
		}
	}
}
