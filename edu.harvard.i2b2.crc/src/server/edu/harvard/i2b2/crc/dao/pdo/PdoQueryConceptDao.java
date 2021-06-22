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

import edu.harvard.i2b2.common.exception.I2B2DAOException;
import edu.harvard.i2b2.common.util.db.JDBCUtil;
import edu.harvard.i2b2.common.util.db.QueryUtil;
import edu.harvard.i2b2.crc.dao.CRCDAO;
import edu.harvard.i2b2.crc.dao.pdo.input.IInputOptionListHandler;
import edu.harvard.i2b2.crc.dao.pdo.input.SQLServerFactRelatedQueryHandler;
import edu.harvard.i2b2.crc.dao.pdo.output.ConceptFactRelated;
import edu.harvard.i2b2.crc.datavo.db.DataSourceLookup;
import edu.harvard.i2b2.crc.datavo.pdo.ConceptSet;
import edu.harvard.i2b2.crc.datavo.pdo.ConceptType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.List;

/**
 * This class handles Concept dimension query's related to PDO request $Id:
 * PdoQueryConceptDao.java,v 1.11 2008/03/19 22:42:08 rk903 Exp $
 * 
 * @author rkuttan
 */
public class PdoQueryConceptDao extends CRCDAO implements IPdoQueryConceptDao {

	private DataSourceLookup dataSourceLookup = null;

	public PdoQueryConceptDao(DataSourceLookup dataSourceLookup,
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
	 * @return {@link ConceptSet}
	 * @throws I2B2DAOException
	 */
	@Override
	public ConceptSet getConceptByConceptCd(List<String> conceptCdList, boolean detailFlag,
											boolean blobFlag, boolean statusFlag) throws I2B2DAOException {
		ConceptSet conceptDimensionSet = new ConceptSet();
		log.debug("Size of input concept cd list " + conceptCdList.size());
		Connection conn = null;
		PreparedStatement query = null;
		String tempTableName = StringUtils.EMPTY;
		try {
			conn = getDataSource().getConnection();
			ConceptFactRelated conceptFactRelated = new ConceptFactRelated(
					buildOutputOptionType(detailFlag, blobFlag, statusFlag));

			String selectClause = conceptFactRelated.getSelectClause();
			log.debug("creating temp table");
			java.sql.Statement tempStmt = conn.createStatement();
			tempTableName = SQLServerFactRelatedQueryHandler.TEMP_PDO_INPUTLIST_TABLE.substring(1);
			try {
				tempStmt.executeUpdate("drop table " + tempTableName);
			} catch (SQLException sqlex) {
				;
			}

			uploadTempTable(tempStmt, tempTableName, conceptCdList);
			String finalSql = "SELECT "
					+ selectClause
					+ " FROM "
					+ getDbSchemaName()
					+ "concept_dimension concept WHERE concept.concept_cd IN (select distinct char_param1 FROM "
					+ tempTableName + ") order by concept_path";
			log.debug("Executing [" + finalSql + "]");

			query = conn.prepareStatement(finalSql);

			ResultSet resultSet = query.executeQuery();

			I2B2PdoFactory.ConceptBuilder conceptBuilder = new I2B2PdoFactory().new ConceptBuilder(
					detailFlag, blobFlag, statusFlag, dataSourceLookup.getServerType());
			while (resultSet.next()) {
				ConceptType conceptDimensionType = conceptBuilder.buildConceptSet(resultSet);
				conceptDimensionSet.getConcept().add(conceptDimensionType);
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
		return conceptDimensionSet;
	}

	/**
	 * Get concept children by item key
	 * 
	 * @param itemKey
	 * @param detailFlag
	 * @param blobFlag
	 * @param statusFlag
	 * @return
	 * @throws I2B2DAOException
	 */
	@Override
	public ConceptSet getChildrentByItemKey(String itemKey, boolean detailFlag, boolean blobFlag, boolean statusFlag)
			throws I2B2DAOException {
		ConceptSet conceptDimensionSet = new ConceptSet();
		if (itemKey != null) {
			if (itemKey.lastIndexOf('\\') == itemKey.length() - 1)
				itemKey = itemKey + "%";
			else {
				log.debug("Adding \\ at the end of the Concept path ");
				itemKey = itemKey + "\\%";
			}
		}
		log.debug("getChildrenByItemKey [" + itemKey + "]");
		Connection conn = null;
		PreparedStatement query = null;
		try {
			conn = getDataSource().getConnection();
			ConceptFactRelated conceptFactRelated = new ConceptFactRelated(buildOutputOptionType(detailFlag, blobFlag, statusFlag));

			String selectClause = conceptFactRelated.getSelectClause();
			String finalSql = "Select * from ( SELECT "
					+ selectClause + " %VID AS RowNum FROM " + getDbSchemaName()
					+ "concept_dimension concept WHERE concept_path " + QueryUtil.getOperatorByValue(itemKey)
					+ " ? order by concept_path)";
			log.debug("Pdo Concept sql [" + finalSql + "]");
			query = conn.prepareStatement(finalSql);
			query.setString(1, QueryUtil.getCleanValue(itemKey));
			ResultSet resultSet = query.executeQuery();

			I2B2PdoFactory.ConceptBuilder conceptBuilder = new I2B2PdoFactory().new ConceptBuilder(
					detailFlag, blobFlag, statusFlag, dataSourceLookup.getServerType());
			while (resultSet.next()) {
				ConceptType conceptDimensionType = conceptBuilder.buildConceptSet(resultSet);
				conceptDimensionSet.getConcept().add(conceptDimensionType);
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
		return conceptDimensionSet;
	}

	private void uploadTempTable(Statement tempStmt, String tempTable, List<String> patientNumList) throws SQLException {
		String createTempInputListTable = "create GLOBAL TEMPORARY table " + tempTable
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
	public ConceptSet getConceptByFact(List<String> panelSqlList, List<Integer> sqlParamCountList,
									   IInputOptionListHandler inputOptionListHandler, boolean detailFlag,
									   boolean blobFlag, boolean statusFlag) throws I2B2DAOException {
		ConceptSet conceptSet = new ConceptSet();
		I2B2PdoFactory.ConceptBuilder conceptBuilder = new I2B2PdoFactory().new ConceptBuilder(
				detailFlag, blobFlag, statusFlag, dataSourceLookup.getServerType());
		ConceptFactRelated conceptFactRelated = new ConceptFactRelated(
				buildOutputOptionType(detailFlag, blobFlag, statusFlag));
		String selectClause = conceptFactRelated.getSelectClause();
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
			String createTempInputListTable = "create GLOBAL TEMPORARY table "
					+ tempTable + " ( set_index int, char_param1 varchar(500) )";

			tempStmt.executeUpdate(createTempInputListTable);
			log.debug("created temp table" + tempTable);
			// if the inputlist is enumeration, then upload the enumerated input
			// to temp table.
			// the uploaded enumerated input will be used in the fact join.
			if (inputOptionListHandler.isEnumerationSet())
				inputOptionListHandler.uploadEnumerationValueToTempTable(conn);

			String insertSql = StringUtils.EMPTY;
			int i = 0;
			int sqlParamCount = 0;
			ResultSet resultSet = null;
			for (String panelSql : panelSqlList) {
				insertSql = " insert into "
						+ tempTable
						+ "(char_param1) select distinct obs_concept_cd from ( "
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
					+ "concept_dimension concept where concept_cd in (select distinct char_param1 from "
					+ tempTable + ") order by concept_path";
			log.debug("Executing SQL [" + finalSql + "]");

			query = conn.prepareStatement(finalSql);
			resultSet = query.executeQuery();

			while (resultSet.next()) {
				ConceptType concept = conceptBuilder.buildConceptSet(resultSet);
				conceptSet.getConcept().add(concept);
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
		return conceptSet;
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
}
