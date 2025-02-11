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
import edu.harvard.i2b2.crc.dao.DAOFactoryHelper;
import edu.harvard.i2b2.crc.dao.setfinder.IQueryResultInstanceDao;
import edu.harvard.i2b2.crc.datavo.db.DataSourceLookup;
import edu.harvard.i2b2.crc.datavo.db.QtQueryResultInstance;
import edu.harvard.i2b2.crc.datavo.pdo.query.EventListType;
import edu.harvard.i2b2.crc.datavo.pdo.query.EventListType.EventId;

/**
 * Handler class for visit/event list type to generate "where" clause for pdo
 * request $Id: VisitListTypeHandler.java,v 1.17 2009/10/23 19:54:02 rk903 Exp $
 * 
 * @author rkuttan
 */
public class VisitListTypeHandler extends CRCDAO implements
		IInputOptionListHandler {
	private EventListType visitListType = null;
	private int minIndex = 0;
	private int maxIndex = 0;
	private String encounterSetCollId = "";
	private List<String> encounterNumList = null;
	private DataSourceLookup dataSourceLookup = null;
	private boolean deleteTempTableFlag = false;

	/**
	 * Constructor accepts {@link EventListType}
	 * 
	 * @param visitListType
	 * @throws I2B2DAOException
	 */
	public VisitListTypeHandler(DataSourceLookup dataSourceLookup,
			EventListType visitListType) throws I2B2DAOException {
		if (visitListType == null) {
			throw new I2B2DAOException("Visit List Type is null");
		}

		this.dataSourceLookup = dataSourceLookup;
		setDbSchemaName(dataSourceLookup.getFullSchema());
		this.visitListType = visitListType;

		if (visitListType.getMin() != null) {
			minIndex = visitListType.getMin();
		}

		if (visitListType.getMax() != null) {
			maxIndex = visitListType.getMax();
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
	public void setMaxIndex(int maxIndex) {
		visitListType.setMax(maxIndex);
	}

	@Override
	public boolean isCollectionId() {
		if (visitListType.getPatientEventCollId() != null) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean isEnumerationSet() {
		if ((visitListType.getEventId() != null)
				&& (visitListType.getEventId().size() > 0)) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean isEntireSet() {
		if (visitListType.getEntireEventSet() != null) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Function to generate "where" clause for visit/event list
	 */
	@Override
	public String generateWhereClauseSql() {
		String sqlString = null;
		if (visitListType.getPatientEventCollId() != null) {
			// set patient set coll id
			this.encounterSetCollId = this.getCollectionId();
			// set sql string
			sqlString = "select eset.encounter_num from "
					+ this.getDbSchemaName()
					+ "qt_patient_enc_collection eset where eset.result_instance_id = ? ";
			if (minIndex <= maxIndex)
				sqlString += (" and eset.set_index between " + minIndex + " and " + maxIndex);
		} else if (visitListType.getEventId() != null && visitListType.getEventId().size() > 0) {
			String tempTableName = this.getTempTableName();
			sqlString = " select char_param1 from " + tempTableName + "  ";
		} else if (visitListType.getEntireEventSet() != null) {
			// by default get first 100 rows
			if (minIndex == 0 && maxIndex == 0) {
				minIndex = 0;
				maxIndex = 100;
			}
			sqlString = " select encounter_num from ( select encounter_num, %VID as rnum  from "
					+ this.getDbSchemaName()
					+ "visit_dimension ) as v "
					+ " where rnum between "
					+ minIndex
					+ " and "
					+ maxIndex + " order by encounter_num";
		}
		return sqlString;
	}

	public String generatePatentSql() {
		String sqlString = null;

		if (visitListType.getPatientEventCollId() != null) {
			// set patient set coll id
			this.encounterSetCollId = visitListType.getPatientEventCollId();
			// set sql string
			sqlString = "select eset.patient_num from " + getDbSchemaName()
					+ "qt_patient_enc_collection eset where eset.result_instance_id = ? ";
			if (minIndex < maxIndex)
				sqlString += (" and eset.set_index between " + minIndex + " and " + maxIndex);
		}
		return sqlString;
	}

	@Override
	public List<String> getEnumerationList() {
		ArrayList<String> encounterNumArrayList = new ArrayList<String>();

		for (EventListType.EventId encounterNum : visitListType.getEventId()) {
			// TODO see if we can use index value from encounterNum
			encounterNumArrayList.add(encounterNum.getValue());
		}

		if (maxIndex > visitListType.getEventId().size()) {
			// log.warn("max size is more than list size");
			maxIndex = visitListType.getEventId().size();
		}

		// set int List
		if (minIndex < maxIndex)
			this.encounterNumList = encounterNumArrayList.subList(minIndex, maxIndex);
		else if (minIndex == maxIndex && minIndex > 0) {
			// check if maxIndex is equal to last index
			if (maxIndex == visitListType.getEventId().size() - 1) {
				this.encounterNumList = new ArrayList();
				this.encounterNumList.add(encounterNumArrayList.get(maxIndex));
			} else
				this.encounterNumList = encounterNumArrayList.subList(minIndex, maxIndex);
		} else {
			maxIndex = encounterNumArrayList.size();
			this.encounterNumList = encounterNumArrayList.subList(minIndex, maxIndex);
		}
		return this.encounterNumList;
	}

	@Override
	public String getCollectionId() {
		if (isCollectionId())
			encounterSetCollId = visitListType.getPatientEventCollId();
		return encounterSetCollId;
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
		if (this.isEnumerationSet())
			return visitListType.getEventId().size();
		else if (this.isCollectionId()) {
			DAOFactoryHelper helper = new DAOFactoryHelper(dataSourceLookup.getDomainId(),
					dataSourceLookup.getProjectPath(),
					dataSourceLookup.getOwnerId());
			IQueryResultInstanceDao resultInstanceDao = helper.getDAOFactory().getSetFinderDAOFactory().getPatientSetResultDAO();

			QtQueryResultInstance resultInstance = resultInstanceDao.getResultInstanceById(this.getCollectionId());
			return resultInstance.getSetSize();
		} else if (this.isEntireSet())
			return 1000;
		else
			return 0;
	}

	@Override
	public void uploadEnumerationValueToTempTable(Connection conn) throws SQLException {
		String tempTableName = this.getTempTableName();
		deleteTempTableFlag = true;
		// create temp table
		java.sql.Statement tempStmt = conn.createStatement();
		int i = 0, j = 1;
		List<EventId> pidList = visitListType.getEventId();
		List<EventId> finalPidList = new ArrayList<EventId>();

		if (maxIndex > visitListType.getEventId().size()) {
			// log.warn("max size is more than list size");
			maxIndex = visitListType.getEventId().size();
		}
		if (minIndex < maxIndex)
			finalPidList = pidList.subList(minIndex, maxIndex);
		else if (minIndex == maxIndex && minIndex > 0) {
			// check if maxIndex is equal to last index
			if (maxIndex == visitListType.getEventId().size() - 1)
				finalPidList.add(pidList.get(maxIndex));
			else
				finalPidList = pidList.subList(minIndex, maxIndex);
		} else {
			maxIndex = pidList.size();
			finalPidList = pidList.subList(minIndex, maxIndex);
		}

		PreparedStatement preparedStmt = conn.prepareStatement("insert into "
				+ tempTableName + "(set_index,char_param1)  values (?,?)");
		for (EventId pid : finalPidList) {
			preparedStmt.setInt(1, j++);
			preparedStmt.setString(2, pid.getValue());
			preparedStmt.addBatch();
			i++;
			if (i % 100 == 0)
				preparedStmt.executeBatch();
		}
		preparedStmt.executeBatch();
	}

	@Override
	public void deleteTempTable(Connection conn) throws SQLException {
		if (!deleteTempTableFlag)
			return;

		Statement deleteStmt = null;
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

	private String getTempTableName() {
		return this.getDbSchemaName() + SQLServerFactRelatedQueryHandler.TEMP_PDO_INPUTLIST_TABLE;
	}
}
