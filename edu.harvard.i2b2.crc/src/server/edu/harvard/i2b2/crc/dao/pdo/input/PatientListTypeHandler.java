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
import edu.harvard.i2b2.crc.datavo.pdo.query.PatientListType;
import edu.harvard.i2b2.crc.datavo.pdo.query.PatientListType.PatientId;
import org.apache.commons.lang3.StringUtils;

/**
 * Handler class for patient list type to generate "where" clause for pdo
 * request $Id: PatientListTypeHandler.java,v 1.8 2008/06/10 14:59:04 rk903 Exp
 * $
 * 
 * @author rkuttan
 */
public class PatientListTypeHandler extends CRCDAO implements IInputOptionListHandler {
	private PatientListType patientListType = null;
	private int minIndex = 0;
	private int maxIndex = 0;
	private String patientSetCollId = StringUtils.EMPTY;
	private List<String> patientNumList = null;
	private DataSourceLookup dataSourceLookup = null;
	private boolean deleteTempTableFlag = false;

	/**
	 * Constructor accepts {@link PatientListType}
	 * 
	 * @param patientListType
	 * @throws I2B2DAOException
	 */
	public PatientListTypeHandler(DataSourceLookup dataSourceLookup,
			final PatientListType patientListType) throws I2B2DAOException {
		if (patientListType == null) {
			throw new I2B2DAOException("Patient List Type is null");
		}
		this.dataSourceLookup = dataSourceLookup;
		this.setDbSchemaName(dataSourceLookup.getFullSchema());
		this.patientListType = patientListType;

		if (patientListType.getMin() != null)
			minIndex = patientListType.getMin();

		if (patientListType.getMax() != null)
			maxIndex = patientListType.getMax();
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
		patientListType.setMax(maxIndex);
	}

	/**
	 * Function to generate "where" clause for patient list
	 */
	@Override
	public String generateMinIndexSql(String panelSql) {
		String sqlString = null;
		log.info("PatientListTypeHandler.class: generateMinIndexSql(String panelSql)");
		if (patientListType.getPatientSetCollId() != null) {
			// set patient set coll id
			this.patientSetCollId = this.getCollectionId();
			String asClause = "as";
			// set sql string
			sqlString = "select min(set_index) ,count(*)  from "
					+ this.getDbSchemaName()
					+ "qt_patient_set_collection pset where pset.result_instance_id =  ?  ";

			if (minIndex <= maxIndex)
				sqlString += (" and pset.set_index between " + minIndex+ " and " + maxIndex);
			sqlString += " and pset.patient_num in (select obs_patient_num from ( "
					+ panelSql + " ) " + asClause + " panelPatientSubQuery ) ";
		} else if (patientListType.getPatientId() != null && patientListType.getPatientId().size() > 0) {
			this.getEnumerationList();
			String asClause = "as";
			String tempTableName = SQLServerFactRelatedQueryHandler.TEMP_PDO_INPUTLIST_TABLE;

			sqlString = " select min(set_index), count(*) from " + this.getDbSchemaName() + tempTableName;
			sqlString += " where ";
			if (minIndex <= maxIndex) {
				if (maxIndex == 1)
					minIndex = 0;
				sqlString += "  set_index between  " + minIndex + " and "
						+ maxIndex;
			}
			sqlString += " and char_param1 in (select obs_patient_num from ( "
					+ panelSql + " )" + asClause + " panelPatientSubQuery )";

		} else if (patientListType.getEntirePatientSet() != null) {
			// by default get first 100 rows
			if (minIndex == 0 && maxIndex == 0) {
				minIndex = 0;
				maxIndex = 100;
			}
			sqlString = "	select patient_num from (select *, %VID as rnum "
					+ " from "
					+ this.getDbSchemaName()
					+ "patient_dimension p) as p1  where rnum between  "
					+ minIndex + "  and  " + maxIndex + " order by patient_num asc";
		}
		log.info("Script: " + sqlString);
		return sqlString;
	}

	/**
	 * Function to generate "where" clause for patient list
	 */
	@Override
	public String generateWhereClauseSql() {
		log.info("PatientListTypeHandler.class: generateWhereClauseSql()");
		String sqlString = null;

		if (patientListType.getPatientSetCollId() != null) {
			// set patient set coll id
			this.patientSetCollId = this.getCollectionId();

			// set sql string
			sqlString = "select pset.patient_num from "
					+ this.getDbSchemaName()
					+ "qt_patient_set_collection pset where pset.result_instance_id =  ?  ";

			if (minIndex <= maxIndex)
				sqlString += (" and pset.set_index between " + minIndex + " and " + maxIndex);
		} else if ((patientListType.getPatientId() != null)
				&& (patientListType.getPatientId().size() > 0)) {
			// this.getEnumerationList();
			String tempTableName = this.getTempTableName();

			sqlString = " select cast(char_param1 as integer) from " + tempTableName + "  ";

		} else if (patientListType.getEntirePatientSet() != null) {
			// by default get first 100 rows
			if ((minIndex == 0) && (maxIndex == 0)) {
				minIndex = 0;
				maxIndex = 100;
			}
			sqlString = "	select patient_num from (select *, %VID as rnum "
					+ " from "
					+ this.getDbSchemaName()
					+ "patient_dimension p) as p1  where rnum between  "
					+ minIndex + "  and  " + maxIndex + " order by patient_num asc";
		}
		log.info("Script: " + sqlString);
		return sqlString;
	}

	public List<String> getIntListFromPatientNumList() {
		return this.patientNumList;
	}

	@Override
	public String getCollectionId() {
		return isCollectionId() ? patientListType.getPatientSetCollId() : StringUtils.EMPTY;
	}

	@Override
	public List<String> getEnumerationList() {
		ArrayList<String> patientNumArrayList = new ArrayList<String>(
				patientListType.getPatientId().size() + 1);
		patientNumArrayList.add(StringUtils.EMPTY);

		// index 0
		// patientNumArrayList.add(StringUtils.EMPTY);
		for (PatientListType.PatientId patientNum : patientListType
				.getPatientId()) {

			// patientNum.getIndex()
			// TODO see if we can use index value from patientNum
			patientNumArrayList.add(patientNum.getValue());
		}

		if (maxIndex >= patientListType.getPatientId().size() + 1) {
			maxIndex = patientListType.getPatientId().size() + 1;
		} else {
			maxIndex += 1;
		}

		// set int List
		if (minIndex < maxIndex)
			this.patientNumList = patientNumArrayList.subList(minIndex, maxIndex);
		else if (minIndex == maxIndex && minIndex > 0) {
			// check if maxIndex is equal to last index
			if (maxIndex == patientListType.getPatientId().size() - 1) {
				this.patientNumList = new ArrayList();
				this.patientNumList.add(patientNumArrayList.get(maxIndex));
			} else
				this.patientNumList = patientNumArrayList.subList(minIndex, maxIndex);
		} else {
			maxIndex = patientNumArrayList.size();
			this.patientNumList = patientNumArrayList.subList(minIndex, maxIndex);
		}
		System.out.println(" MAX INDEX *** " + maxIndex);
		return this.patientNumList;
	}

	@Override
	public boolean isCollectionId() {
		return patientListType.getPatientSetCollId() != null;
	}

	@Override
	public boolean isEntireSet() {
		return patientListType.getEntirePatientSet() != null;
	}

	@Override
	public boolean isEnumerationSet() {
		return (patientListType.getPatientId() != null)
				&& (patientListType.getPatientId().size() > 0);
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
			return patientListType.getPatientId().size();
		else if (this.isCollectionId()) {
			DAOFactoryHelper helper = new DAOFactoryHelper(dataSourceLookup
					.getDomainId(), dataSourceLookup.getProjectPath(),
					dataSourceLookup.getOwnerId());
			IQueryResultInstanceDao resultInstanceDao = helper.getDAOFactory()
					.getSetFinderDAOFactory().getPatientSetResultDAO();

			QtQueryResultInstance resultInstance = resultInstanceDao
					.getResultInstanceById(this.getCollectionId());
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
		deleteTempTable(conn);
		// create temp table
		java.sql.Statement tempStmt = conn.createStatement();

		String createTempInputListTable = "create GLOBAL TEMPORARY table "
				+ getTempTableName()
				+ " (set_index int, char_param1 varchar(100) )";
		tempStmt.executeUpdate(createTempInputListTable);
		int i = 0, j = 1;
		List<PatientId> pidList = patientListType.getPatientId();
		List<PatientId> finalPidList = new ArrayList<PatientId>();
		if (maxIndex > patientListType.getPatientId().size()) {
			// log.warn("max size is more than list size");
			maxIndex = patientListType.getPatientId().size();
		}
		if (minIndex < maxIndex)
			finalPidList = pidList.subList(minIndex, maxIndex);
		else if (minIndex == maxIndex && minIndex > 0) {
			// check if maxIndex is equal to last index
			if (maxIndex == patientListType.getPatientId().size() - 1)
				finalPidList.add(pidList.get(maxIndex));
			else
				finalPidList = pidList.subList(minIndex, maxIndex);
		} else {
			maxIndex = pidList.size();
			finalPidList = pidList.subList(minIndex, maxIndex);
		}
		PreparedStatement preparedStmt = conn.prepareStatement("insert into "
				+ tempTableName + "(set_index,char_param1)  values (?,?)");
		for (PatientId pid : finalPidList) {
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
			//throw sqle;
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
		return SQLServerFactRelatedQueryHandler.TEMP_PDO_INPUTLIST_TABLE.substring(1);
	}
}
