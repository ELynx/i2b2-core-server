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
package edu.harvard.i2b2.crc.dao.pdo.input;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import edu.harvard.i2b2.common.exception.I2B2DAOException;
import edu.harvard.i2b2.crc.dao.CRCDAO;
import edu.harvard.i2b2.crc.datavo.db.DataSourceLookup;
import edu.harvard.i2b2.crc.datavo.pdo.query.EidListType;
import edu.harvard.i2b2.crc.datavo.pdo.query.EventListType;
import edu.harvard.i2b2.crc.datavo.pdo.query.EidListType.Eid;
import org.apache.commons.lang3.StringUtils;

/**
 * Handler class for pid list type to generate "where" clause for pdo request
 * $Id: EidListTypeHandler.java,v 1.3 2009/10/23 19:54:02 rk903 Exp $
 * 
 * @author rkuttan
 */
public class EidListTypeHandler extends CRCDAO implements
		IInputOptionListHandler {
	private EidListType eidListType = null;
	private int minIndex = 0;
	private int maxIndex = 0;
	private String encounterSetCollId = StringUtils.EMPTY;
	private List<String> encounterNumList = null;
	private DataSourceLookup dataSourceLookup = null;
	private boolean deleteTempFlag = false;

	/**
	 * Constructor accepts {@link EventListType}
	 * 
	 * @param visitListType
	 * @throws I2B2DAOException
	 */
	public EidListTypeHandler(DataSourceLookup dataSourceLookup,
			EidListType eidListType) throws I2B2DAOException {
		if (eidListType == null) {
			throw new I2B2DAOException("Eid List Type is null");
		}

		this.dataSourceLookup = dataSourceLookup;
		setDbSchemaName(dataSourceLookup.getFullSchema());
		this.eidListType = eidListType;

		if (eidListType.getMin() != null) {
			minIndex = eidListType.getMin();
		}

		if (eidListType.getMax() != null) {
			maxIndex = eidListType.getMax();
		}
	}

	@Override
	public int getMinIndex() {
		return minIndex;
	}

	@Override
	public int getMaxIndex() {
		return maxIndex;
	}

	@Override
	public boolean isCollectionId() {
		return false;
	}

	@Override
	public boolean isEnumerationSet() {
		return (eidListType.getEid() != null) && (eidListType.getEid().size() > 0);
	}

	@Override
	public boolean isEntireSet() {
		return false;
	}

	/**
	 * Function to generate "where" clause for visit/event list
	 */
	@Override
	public String generateWhereClauseSql() {

		String tempTableName = getTempTableName();

		return "SELECT "
				+ " em.encounter_num "
				+ " FROM "
				+ getDbSchemaName()
				+ "encounter_mapping em WHERE  exists (select  1 FROM "
				+ tempTableName
				+ " where char_param1 = em.encounter_ide and char_param2 = em.encounter_ide_source)  ";
	}

	public String generatePatentSql() {
		String sqlString = null;

		return sqlString;
	}

	@Override
	public List<String> getEnumerationList() {
		ArrayList<String> encounterNumArrayList = new ArrayList<String>();
		return this.encounterNumList;
	}

	@Override
	public void uploadEnumerationValueToTempTable(Connection conn)
			throws SQLException {

		String tempTableName = this.getTempTableName();

		// create temp table
		java.sql.Statement tempStmt = conn.createStatement();

		String createTempInputListTable = "create GLOBAL TEMPORARY table "
				+ getTempTableName()
				+ " (set_index int, char_param1 varchar(100), char_param2 varchar(100) )";
		deleteTempFlag = true;
		tempStmt.executeUpdate(createTempInputListTable);
			
		int i = 0, j = 1;

		List<Eid> eidList = eidListType.getEid();
		List<Eid> finalEidList = new ArrayList<Eid>();
		if (maxIndex > eidListType.getEid().size()) {
			// log.warn("max size is more than list size");
			maxIndex = eidListType.getEid().size();
		}
		if (minIndex < maxIndex) {
			finalEidList = eidList.subList(minIndex, maxIndex);
		} else if (minIndex == maxIndex && minIndex > 0) {
			// check if maxIndex is equal to last index
			if (maxIndex == eidListType.getEid().size() - 1) 
				finalEidList.add(eidList.get(maxIndex));
			 else 
				finalEidList = eidList.subList(minIndex, maxIndex);

		} else {
			maxIndex = eidList.size();
			finalEidList = eidList.subList(minIndex, maxIndex);
		}

		PreparedStatement preparedStmt = conn.prepareStatement("insert into " + tempTableName
				+ "(set_index,char_param1,char_param2)  values (?,?,?)");
		for (Eid eid : finalEidList) {
			preparedStmt.setInt(1, j++);
			preparedStmt.setString(2, eid.getValue());
			preparedStmt.setString(3, eid.getSource());
			preparedStmt.addBatch();
			i++;
			if (i % 100 == 0) 
				preparedStmt.executeBatch();
			log.debug("loading " + eid.getValue() + " " + eid.getSource());
		}
		preparedStmt.executeBatch();
	}

	@Override
	public void deleteTempTable(Connection conn) throws SQLException {
		Statement deleteStmt = null;
		if (!deleteTempFlag) 
			return;
		
		try {
			deleteStmt = conn.createStatement();
			deleteStmt.executeUpdate("drop table " + getTempTableName());
		} catch (SQLException sqle) {
			throw sqle;
		} finally {
			try {
				if(deleteStmt != null)
					deleteStmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public String getCollectionId() {
		return StringUtils.EMPTY;
	}

	@Override
	public String generateMinIndexSql(String panelSql) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Returns input list's size. if the list is collection id, then collection
	 * set size, if the list is entire set, then total rows in dimension table
	 * if the list is enumeration, then size of enumeration set
	 * 
	 * @return
	 * @throws I2B2DAOException
	 */
	@Override
	public int getInputSize() throws I2B2DAOException {
		return 0;
	}

	@Override
	public void setMaxIndex(int maxIndex) {
		eidListType.setMax(maxIndex);
	}

	public String getTempTableName() {
		return this.getDbSchemaName()
					+ SQLServerFactRelatedQueryHandler.TEMP_PDO_INPUTLIST_TABLE;
	}
}
