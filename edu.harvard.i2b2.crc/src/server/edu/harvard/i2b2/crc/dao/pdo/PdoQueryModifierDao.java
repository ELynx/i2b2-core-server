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
package edu.harvard.i2b2.crc.dao.pdo;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import oracle.sql.ArrayDescriptor;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.jboss.resource.adapter.jdbc.WrappedConnection;

import edu.harvard.i2b2.common.exception.I2B2DAOException;
import edu.harvard.i2b2.common.util.db.JDBCUtil;
import edu.harvard.i2b2.crc.dao.CRCDAO;
import edu.harvard.i2b2.crc.dao.DAOFactoryHelper;
import edu.harvard.i2b2.crc.dao.pdo.input.FactRelatedQueryHandler;
import edu.harvard.i2b2.crc.dao.pdo.input.IInputOptionListHandler;
import edu.harvard.i2b2.crc.dao.pdo.input.SQLServerFactRelatedQueryHandler;
import edu.harvard.i2b2.crc.dao.pdo.output.ModifierFactRelated;
import edu.harvard.i2b2.crc.datavo.db.DataSourceLookup;
import edu.harvard.i2b2.crc.datavo.pdo.ModifierSet;
import edu.harvard.i2b2.crc.datavo.pdo.ModifierType;
import edu.harvard.i2b2.crc.datavo.pdo.PatientDataType;

/**
 * This class handles Concept dimension query's related to PDO request $Id:
 * PdoQueryConceptDao.java,v 1.11 2008/03/19 22:42:08 rk903 Exp $
 * 
 * @author rkuttan
 */
public class PdoQueryModifierDao extends CRCDAO implements IPdoQueryModifierDao {

	private DataSourceLookup dataSourceLookup = null;

	public PdoQueryModifierDao(DataSourceLookup dataSourceLookup,
			DataSource dataSource) {
		this.dataSourceLookup = dataSourceLookup;
		setDataSource(dataSource);
		setDbSchemaName(dataSourceLookup.getFullSchema());
	}

	/** log * */
	protected final Log log = LogFactory.getLog(getClass());

	/**
	 * Get concepts detail from concept code list
	 * 
	 * @param conceptCdList
	 * @param detailFlag
	 * @param blobFlag
	 * @param statusFlag
	 * @return {@link PatientDataType.ConceptDimensionSet}
	 * @throws I2B2DAOException
	 */
	@Override
	public ModifierSet getModifierByModifierCd(List<String> modifierCdList,
			boolean detailFlag, boolean blobFlag, boolean statusFlag)
			throws I2B2DAOException {

		ModifierSet modifierDimensionSet = new ModifierSet();
		log.debug("Size of input modifier cd list " + modifierCdList.size());
		Connection conn = null;
		PreparedStatement query = null;
		String tempTableName = "";
		try {
			conn = getDataSource().getConnection();
			ModifierFactRelated modifierFactRelated = new ModifierFactRelated(
					buildOutputOptionType(detailFlag, blobFlag, statusFlag));

			String selectClause = modifierFactRelated.getSelectClause();
			String serverType = dataSourceLookup.getServerType();
			log.debug("creating temp table");
			java.sql.Statement tempStmt = conn.createStatement();
			tempTableName = SQLServerFactRelatedQueryHandler.TEMP_PDO_INPUTLIST_TABLE.substring(1);

			try {
				tempStmt.executeUpdate("drop table " + tempTableName);
			} catch (SQLException sqlex) {
				;
			}

			uploadTempTable(tempStmt, tempTableName, modifierCdList);
			String finalSql = "SELECT "
					+ selectClause
					+ " FROM "
					+ getDbSchemaName()
					+ "modifier_dimension modifier WHERE modifier.modifier_cd IN (select distinct char_param1 FROM "
					+ tempTableName + ") order by modifier_path";
			log.debug("Executing [" + finalSql + "]");

			query = conn.prepareStatement(finalSql);
			ResultSet resultSet = query.executeQuery();
			I2B2PdoFactory.ModifierBuilder modifierBuilder = new I2B2PdoFactory().new ModifierBuilder(
					detailFlag, blobFlag, statusFlag, dataSourceLookup.getServerType());
			while (resultSet.next()) {
				ModifierType modifierDimensionType = modifierBuilder.buildModifierSet(resultSet);
				modifierDimensionSet.getModifier().add(modifierDimensionType);
			}
		} catch (SQLException sqlEx) {
			log.error(StringUtils.EMPTY, sqlEx);
			throw new I2B2DAOException(StringUtils.EMPTY, sqlEx);
		} catch (IOException ioEx) {
			log.error(StringUtils.EMPTY, ioEx);
			throw new I2B2DAOException(StringUtils.EMPTY, ioEx);
		} finally {
			try {
				JDBCUtil.closeJdbcResource(null, query, conn);
			} catch (SQLException sqlEx) {
				sqlEx.printStackTrace();
			}
		}
		return modifierDimensionSet;
	}

	private void uploadTempTable(Statement tempStmt, String tempTable,
								 List<String> patientNumList) throws SQLException {
		String createTempInputListTable =  "create GLOBAL TEMPORARY table " + tempTable
				+ " ( char_param1 varchar(100) )";
		tempStmt.executeUpdate(createTempInputListTable);
		log.debug("created temp table" + tempTable);
		PreparedStatement preparedStmt = tempStmt.getConnection()
				.prepareStatement("insert into " + tempTable + " values (?)");
		// load to temp table
		// TempInputListInsert inputListInserter = new
		// TempInputListInsert(dataSource,TEMP_PDO_INPUTLIST_TABLE);
		// inputListInserter.setBatchSize(100);
		int i = 0;
		for (String singleValue : patientNumList) {
			preparedStmt.setString(1, singleValue);
			preparedStmt.addBatch();
			log.debug("adding batch [" + i + "] " + singleValue);
			i++;
			if (i % 100 == 0) {
				log.debug("batch insert [" + i + "]");
				preparedStmt.executeBatch();
			}
		}
		log.debug("batch insert [" + i + "]");
		preparedStmt.executeBatch();
	}

	@Override
	public ModifierSet getModifierByFact(List<String> panelSqlList,
										 List<Integer> sqlParamCountList,
										 IInputOptionListHandler inputOptionListHandler, boolean detailFlag,
										 boolean blobFlag, boolean statusFlag) throws I2B2DAOException {
		ModifierSet modifierSet = new ModifierSet();
		I2B2PdoFactory.ModifierBuilder modifierBuilder = new I2B2PdoFactory().new ModifierBuilder(
				detailFlag, blobFlag, statusFlag, dataSourceLookup.getServerType());
		ModifierFactRelated modifierFactRelated = new ModifierFactRelated(
				buildOutputOptionType(detailFlag, blobFlag, statusFlag));
		String selectClause = modifierFactRelated.getSelectClause();
		String tempTable = StringUtils.EMPTY;
		Connection conn = null;
		PreparedStatement query = null;
		try {
			conn = dataSource.getConnection();
			log.debug("creating temp table");
			java.sql.Statement tempStmt = conn.createStatement();
			tempTable = SQLServerFactRelatedQueryHandler.TEMP_FACT_PARAM_TABLE.substring(1);

			try {
				tempStmt.executeUpdate("drop table " + tempTable);
			} catch (SQLException sqlex) {
				;
			}
			String createTempInputListTable = "create table " + tempTable
					+ " ( set_index int, char_param1 varchar(500) )";
			tempStmt.executeUpdate(createTempInputListTable);
			log.debug("created temp table" + tempTable);

			// if the inputlist is enumeration, then upload the enumerated input
			// to temp table.
			// the uploaded enumerated input will be used in the fact join.
			if (inputOptionListHandler.isEnumerationSet())
				inputOptionListHandler.uploadEnumerationValueToTempTable(conn);
			String insertSql;
			int i = 0;
			int sqlParamCount = 0;
			ResultSet resultSet;
			for (String panelSql : panelSqlList) {
				insertSql = " insert into "
						+ tempTable
						+ "(char_param1) select distinct obs_modifier_cd from ( "
						+ panelSql + ") b";
				log.debug("Executing SQL [ " + insertSql + "]");
				sqlParamCount = sqlParamCountList.get(i++);
				// conn.createStatement().executeUpdate(insertSql);
				executeTotalSql(insertSql, conn, sqlParamCount, inputOptionListHandler);
			}

			String finalSql = "SELECT "
					+ selectClause
					+ " FROM "
					+ getDbSchemaName()
					+ "modifier_dimension modifier where modifier_cd in (select distinct char_param1 from "
					+ tempTable + ") order by modifier_path";
			log.debug("Executing SQL [" + finalSql + "]");
			System.out.println("Final Sql " + finalSql);

			query = conn.prepareStatement(finalSql);
			resultSet = query.executeQuery();
			while (resultSet.next()) {
				ModifierType modifier = modifierBuilder.buildModifierSet(resultSet);
				modifierSet.getModifier().add(modifier);
			}
		} catch (SQLException sqlEx) {
			log.error(StringUtils.EMPTY, sqlEx);
			throw new I2B2DAOException("sql exception", sqlEx);
		} catch (IOException ioEx) {
			log.error(StringUtils.EMPTY, ioEx);
			throw new I2B2DAOException("IO exception", ioEx);
		} finally {
			PdoTempTableUtil tempUtil = new PdoTempTableUtil();
			tempUtil.clearTempTable(dataSourceLookup.getServerType(), conn, tempTable);
			if (inputOptionListHandler != null && inputOptionListHandler.isEnumerationSet()) {
				try {
					inputOptionListHandler.deleteTempTable(conn);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			try {
				JDBCUtil.closeJdbcResource(null, query, conn);
			} catch (SQLException sqlEx) {
				sqlEx.printStackTrace();
			}
		}
		return modifierSet;
	}

	private void executeTotalSql(String totalSql, Connection conn,
			int sqlParamCount, IInputOptionListHandler inputOptionListHandler)
			throws SQLException {
		PreparedStatement stmt = conn.prepareStatement(totalSql);
		System.out.println(totalSql + " [ " + sqlParamCount + " ]");
		if (inputOptionListHandler.isCollectionId()) {
			for (int i = 1; i <= sqlParamCount; i++) {
				stmt.setInt(i, Integer.parseInt(inputOptionListHandler
						.getCollectionId()));
			}
		}
		stmt.executeUpdate();
	}
}
