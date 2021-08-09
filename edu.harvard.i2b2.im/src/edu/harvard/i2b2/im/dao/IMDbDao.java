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
 * 		Lori Phillips
 */
package edu.harvard.i2b2.im.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import edu.harvard.i2b2.common.util.db.QueryUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import edu.harvard.i2b2.common.exception.I2B2DAOException;
import edu.harvard.i2b2.common.exception.I2B2Exception;
import edu.harvard.i2b2.im.ejb.DBInfoType;
import edu.harvard.i2b2.im.util.IMUtil;


public class IMDbDao extends JdbcDaoSupport {

	private static Log log = LogFactory.getLog(IMDbDao.class);

	private JdbcTemplate jt;

	public IMDbDao() throws I2B2Exception{
		DataSource ds = null;
		try {
			ds = IMUtil.getInstance().getDataSource("java:/IrisDS");
			//		log.info(ds.toString());
		} catch (I2B2Exception e2) {
			log.error("bootstrap ds failure: " + e2.getMessage());
			throw e2;
		} 
		this.jt = new JdbcTemplate(ds);
	}

	private String getIMSchema() throws I2B2Exception {
		return IMUtil.getInstance().getIMDataSchemaName();
	}

	public List<DBInfoType> getDbLookupByHiveOwner(String domainId,String ownerId) throws I2B2Exception, I2B2DAOException {
		String metadataSchema = getIMSchema();
		String sql =  "select * from " + metadataSchema + "im_db_lookup where LOWER(c_domain_id) = ? " +
				"and c_project_path = ? and (LOWER(c_owner_id) = ? or c_owner_id ='@') order by c_project_path";
		String projectId = "@";
		//		log.info(sql + domainId + projectId + ownerId);
		List queryResult;
		try {
			queryResult = jt.query(sql, new getDBMapper(), domainId.toLowerCase(),projectId,ownerId.toLowerCase());
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			throw new I2B2DAOException("Database error: "+ e.getMessage());
		}
		return queryResult;
		//		List<DBInfoType> dataSourceLookupList = 
		//			this.query(sql, new Object[]{domainId,projectId,ownerId}, new mapper());
		//		return dataSourceLookupList;
	}

	@SuppressWarnings("unchecked")
	public List<DBInfoType> getDbLookupByHiveProjectOwner(String domainId, String projectId, String ownerId)
			throws I2B2Exception, I2B2DAOException{
		String metadataSchema = getIMSchema();
		String sql = "select * from " + metadataSchema + "im_db_lookup where LOWER(c_domain_id) = ? " +
				"and LOWER(c_project_path) " + QueryUtil.getOperatorByValue(projectId.toLowerCase()) +
				" ? and (LOWER(c_owner_id) = ? or c_owner_id = '@') order by c_project_path"; // desc  c_owner_id desc";
		List queryResult;
		try {
			queryResult = jt.query(sql, new getDBMapper(), domainId.toLowerCase(),
					QueryUtil.getCleanValue(projectId.toLowerCase()), ownerId.toLowerCase());
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			throw new I2B2DAOException("Database error:" + e.getMessage());
		}
		return queryResult;
	}
}

class getDBMapper implements RowMapper<DBInfoType> {
	@Override
	public DBInfoType mapRow(ResultSet rs, int rowNum) throws SQLException {
		DBInfoType dataSourceLookup = new DBInfoType();
		dataSourceLookup.setHive(rs.getString("c_domain_id"));
		dataSourceLookup.setProjectId(rs.getString("c_project_path"));
		dataSourceLookup.setOwnerId(rs.getString("c_owner_id"));
	  //dataSourceLookup.setDatabaseName(rs.getString("c_db_datasource"));
		dataSourceLookup.setDb_serverType("INTERSYSTEMS IRIS");
		dataSourceLookup.setDb_fullSchema(rs.getString("c_db_fullschema"));
		dataSourceLookup.setDb_dataSource(rs.getString("c_db_datasource"));
		dataSourceLookup.setDb_serverType(rs.getString("c_db_servertype"));
		return dataSourceLookup;
	}
}
