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

import edu.harvard.i2b2.common.exception.I2B2DAOException;
import edu.harvard.i2b2.common.util.db.JDBCUtil;
import edu.harvard.i2b2.crc.dao.CRCDAO;
import edu.harvard.i2b2.crc.dao.pdo.input.IInputOptionListHandler;
import edu.harvard.i2b2.crc.dao.pdo.input.SQLServerFactRelatedQueryHandler;
import edu.harvard.i2b2.crc.dao.pdo.output.ModifierFactRelated;
import edu.harvard.i2b2.crc.datavo.db.DataSourceLookup;
import edu.harvard.i2b2.crc.datavo.pdo.ModifierSet;
import edu.harvard.i2b2.crc.datavo.pdo.ModifierType;
import org.apache.commons.lang3.StringUtils;

/**
 * Class to support concept section of table pdo query $Id:
 * TablePdoQueryConceptDao.java,v 1.13 2008/07/21 19:53:40 rk903 Exp $
 * 
 * @author rkuttan
 */
public class TablePdoQueryModifierDao extends CRCDAO implements ITablePdoQueryModifierDao {

	private DataSourceLookup dataSourceLookup = null;
	private String schemaName = null;

	public TablePdoQueryModifierDao(DataSourceLookup dataSourceLookup, DataSource dataSource) {
		this.dataSourceLookup = dataSourceLookup;
		this.setDbSchemaName(dataSourceLookup.getFullSchema());
		setDataSource(dataSource);
	}

	@Override
	public ModifierSet getModifierByFact(List<String> panelSqlList,
										 List<Integer> sqlParamCountList,
										 IInputOptionListHandler inputOptionListHandler,
										 boolean detailFlag, boolean blobFlag,
										 boolean statusFlag) throws I2B2DAOException {
		ModifierSet modifierSet = new ModifierSet();
		RPDRPdoFactory.ModifierBuilder modifierBuilder = new RPDRPdoFactory.ModifierBuilder(
				detailFlag, blobFlag, statusFlag);
		ModifierFactRelated modifierFactRelated = new ModifierFactRelated(
				buildOutputOptionType(detailFlag, blobFlag, statusFlag));
		String selectClause = modifierFactRelated.getSelectClause();
		String serverType = dataSourceLookup.getServerType();
		String factTempTable = StringUtils.EMPTY;
		Connection conn = null;
		PreparedStatement query = null;
		try {
			conn = dataSource.getConnection();
			log.debug("creating temp table");
			java.sql.Statement tempStmt = conn.createStatement();
			factTempTable = this.getDbSchemaName() + SQLServerFactRelatedQueryHandler.TEMP_FACT_PARAM_TABLE;
			try {
				tempStmt.executeUpdate("drop table " + factTempTable);
			} catch (SQLException sqlex) {
				;
			}
			String createTempInputListTable = "create table "
					+ factTempTable
					+ " ( set_index int, char_param1 varchar(500) )";
			tempStmt.executeUpdate(createTempInputListTable);
			log.debug("created temp table" + factTempTable);
			
			// if the inputlist is enumeration, then upload the enumerated input
			// to temp table.
			// the uploaded enumerated input will be used in the fact join.
			if (inputOptionListHandler.isEnumerationSet()) {
				inputOptionListHandler.uploadEnumerationValueToTempTable(conn);
			}
			String insertSql;
			int i = 0;
			int sqlParamCount = 0;
			ResultSet resultSet;
			for (String panelSql : panelSqlList) {
				insertSql = " insert into "
						+ factTempTable
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
					+ factTempTable + ") order by modifier_path";
			log.debug("Executing SQL [" + finalSql + "]");

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
			PdoTempTableUtil tempTableUtil = new PdoTempTableUtil(); 
			tempTableUtil.clearTempTable(serverType, conn, factTempTable);
			
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

	private void executeTotalSql(String totalSql, Connection conn, int sqlParamCount,
								 IInputOptionListHandler inputOptionListHandler) throws SQLException {
		PreparedStatement stmt = conn.prepareStatement(totalSql);
		log.debug(totalSql + " [ " + sqlParamCount + " ]");
		if (inputOptionListHandler.isCollectionId()) {
			for (int i = 1; i <= sqlParamCount; i++)
				stmt.setInt(i, Integer.parseInt(inputOptionListHandler.getCollectionId()));
		}
		stmt.executeUpdate();
	}

	/**
	 * Function returns concepts based on list of concept codes
	 * 
	 * @param modifierCdList
	 * @param detailFlag
	 * @param blobFlag
	 * @param statusFlag
	 * @return ConceptSet
	 * @throws I2B2DAOException
	 */
	@Override
	public ModifierSet getModifierByModifierCd(List<String> modifierCdList, boolean detailFlag, 
											   boolean blobFlag, boolean statusFlag) throws I2B2DAOException {
		ModifierSet modifierSet = new ModifierSet();
		RPDRPdoFactory.ModifierBuilder modifierBuilder = new RPDRPdoFactory.ModifierBuilder(
				detailFlag, blobFlag, statusFlag);
		log.debug("input concept cd list size " + modifierCdList.size());
		Connection conn = null;
		PreparedStatement query = null;
		String tempTableName = StringUtils.EMPTY;
		try {
			conn = getDataSource().getConnection();
			ModifierFactRelated modifierFactRelated = new ModifierFactRelated(
					buildOutputOptionType(detailFlag, blobFlag, statusFlag));
			String selectClause = modifierFactRelated.getSelectClause();
			log.debug("creating temp table");
			java.sql.Statement tempStmt = conn.createStatement();
			tempTableName = this.getDbSchemaName()
					+ SQLServerFactRelatedQueryHandler.TEMP_PDO_INPUTLIST_TABLE;
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
			PdoTempTableUtil tempTableUtil = new PdoTempTableUtil();
			tempTableUtil.deleteTempTableSqlServer(conn, tempTableName);
			
			try {
				JDBCUtil.closeJdbcResource(null, query, conn);
			} catch (SQLException sqlEx) {
				sqlEx.printStackTrace();
			}
		}
		return modifierSet;
	}

	private void uploadTempTable(Statement tempStmt, String tempTableName,
								 List<String> patientNumList) throws SQLException {
		String createTempInputListTable = "create table " + tempTableName
				+ " ( char_param1 varchar(100) )";
		tempStmt.executeUpdate(createTempInputListTable);
		log.debug("created temp table" + tempTableName);
		// load to temp table
		// TempInputListInsert inputListInserter = new
		// TempInputListInsert(dataSource,TEMP_PDO_INPUTLIST_TABLE);
		// inputListInserter.setBatchSize(100);
		int i = 0;
		for (String singleValue : patientNumList) {
			tempStmt.addBatch("insert into " + tempTableName + " values ('"
					+ singleValue + "' )");
			log.debug("adding batch" + singleValue);
			i++;
			if (i % 100 == 0) {
				log.debug("batch insert");
				tempStmt.executeBatch();
			}
		}
		log.debug("batch insert1");
		tempStmt.executeBatch();
	}
}
