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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

import javax.sql.DataSource;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import edu.harvard.i2b2.crc.dao.CRCDAO;
import edu.harvard.i2b2.crc.datavo.db.DataSourceLookup;
import edu.harvard.i2b2.crc.datavo.db.QtPatientSetCollection;
import edu.harvard.i2b2.crc.datavo.db.QtQueryResultInstance;

/**
 * Class to support batch inserts of patients to PatientSetCollection This class
 * uses single Session to persist all the patients to collection. To have
 * limited memory usage, it will flush and clear the session manually for every
 * 1000 inserts. $Id: PatientSetCollectionDao.java,v 1.4 2007/08/31 14:44:26
 * rk903 Exp $
 * 
 * @author rkuttan
 * @see QtPatientSetCollection
 */
public class PatientSetCollectionSpringDao extends CRCDAO implements  IPatientSetCollectionDao {
	/** patient set collection index * */
	private int setIndex = 0, batchDataIndex = 0;
	private JdbcTemplate jdbcTemplate = null;
	private String insert_sql = ""; 
	

	QtPatientSetCollection[] patientSetColl = null;
	/** master table for patient set collection * */
	QtQueryResultInstance resultInstance = null;
	String resultInstanceId = null;
	private static final int INITIAL_ARRAY_SIZE = 1100; 
	private DataSourceLookup dataSourceLookup = null;

	private SQLServerSequenceDAO sqlServerSequenceDao = null;
	/**
	 * Construc with patientset id Initialize hibernate session and creates
	 * Query reuslt instance class
	 * 
	 * @param patientSetId
	 */
	public PatientSetCollectionSpringDao(DataSource dataSource,DataSourceLookup dataSourceLookup) {
		setDataSource(dataSource);
		setDbSchemaName(dataSourceLookup.getFullSchema());
		jdbcTemplate = new JdbcTemplate(dataSource);
		this.dataSourceLookup = dataSourceLookup;
		insert_sql = "insert into " + getDbSchemaName() + "qt_patient_set_collection(result_instance_id,set_index,patient_num) values (?,?,?)";
		sqlServerSequenceDao  = new SQLServerSequenceDAO(dataSource,dataSourceLookup) ;
		resultInstance = new QtQueryResultInstance();
		patientSetColl = new QtPatientSetCollection[INITIAL_ARRAY_SIZE];
	}
	
	@Override
	public void createPatientSetCollection(String resultInstanceId) {
		resultInstance = new QtQueryResultInstance();
		resultInstance.setResultInstanceId(resultInstanceId);
	}

	@Override
	public String getResultInstanceId() {
		return resultInstance.getResultInstanceId();
	}

	/**
	 * function to add patient to patient set without out creating new db
	 * session
	 * 
	 * @param patientId
	 */
	@Override
	public void addPatient(long patientId) {
		setIndex++;

		QtPatientSetCollection collElement = new QtPatientSetCollection();
		int patientSetCollId = 0;
		collElement.setPatientId(patientId);
		collElement.setQtQueryResultInstance(resultInstance);
		collElement.setSetIndex(setIndex);
		patientSetColl[batchDataIndex++] = collElement;

		if ((setIndex % 1000) == 0) {
			InsertStatementSetter batchSetter = new InsertStatementSetter(
					patientSetColl, batchDataIndex);
			jdbcTemplate.batchUpdate(insert_sql, batchSetter);
			Arrays.fill(patientSetColl, null);
			batchDataIndex = 0;
		}
	}

	/**
	 * Call this function at the end. i.e. after loading all patient with
	 * addPatient function, finally call this function to clear session
	 */
	@Override
	public void flush() {
		InsertStatementSetter batchSetter = new InsertStatementSetter(
				patientSetColl, batchDataIndex);
		jdbcTemplate.batchUpdate(insert_sql, batchSetter);
		Arrays.fill(patientSetColl, null);
		batchDataIndex = 0;
		setIndex = 0;
	}

	class InsertStatementSetter implements BatchPreparedStatementSetter {
		private QtPatientSetCollection[] data;
		private int batchSize = 0;

		public InsertStatementSetter(QtPatientSetCollection[] data, int batchSize) {
			this.data = data;
			this.batchSize = batchSize;
		}

		@Override
		public int getBatchSize() {
			return batchSize;
		}

		// this is called for each row
		@Override
		public void setValues(PreparedStatement ps, int i) throws SQLException {
			//ps.setLong(1, data[i].getPatientSetCollId()); // set first value
			ps.setInt(1, Integer.parseInt(data[i].getQtQueryResultInstance()
					.getResultInstanceId()));
			ps.setInt(2, data[i].getSetIndex());
			ps.setLong(3, data[i].getPatientId());
		}
	}
}
