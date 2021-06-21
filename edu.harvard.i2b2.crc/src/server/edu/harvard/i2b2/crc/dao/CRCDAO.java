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
package edu.harvard.i2b2.crc.dao;

import edu.harvard.i2b2.common.exception.I2B2DAOException;
import edu.harvard.i2b2.common.exception.I2B2Exception;
import edu.harvard.i2b2.common.util.ServiceLocator;
import edu.harvard.i2b2.crc.datavo.pdo.query.OutputOptionType;

import edu.harvard.i2b2.crc.util.SqlClauseUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * DAO abstract class to provide common dao functions $Id: CRCDAO.java,v 1.8
 * 2008/03/27 23:38:50 rk903 Exp $
 * 
 * @author rkuttan
 * @see Connection
 * @see Session
 */
public abstract class CRCDAO {
	/** log * */
	protected final Log log = LogFactory.getLog(CRCDAO.class);

	protected DataSource dataSource = null;

	protected String dbSchemaName = null;

	/**
	 * Returns connection from appserver datasource
	 * 
	 * @return Appserver database Connection
	 * @throws SQLException
	 * @throws I2B2DAOException
	 * @throws SQLException
	 */
	protected DataSource getApplicationDataSource(String dataSourceName)
			throws I2B2DAOException {
		try {
			DataSource dataSource = ServiceLocator.getInstance().getAppServerDataSource(dataSourceName);
			return dataSource;
		} catch (I2B2Exception i2b2Ex) {
			log.error(i2b2Ex);
			throw new I2B2DAOException( "Error getting appliation/spring datasource "
							+ dataSourceName + " : " + i2b2Ex.getMessage(), i2b2Ex);
		}
	}

	/**
	 * Helper function to construct {@link OutputOptionType} from given boolean
	 * flags
	 * 
	 * @param detailFlag
	 * @param blobFlag
	 * @param statusFlag
	 * @return OutputOptionType
	 */
	protected OutputOptionType buildOutputOptionType(boolean detailFlag,
													 boolean blobFlag,
													 boolean statusFlag) {
		OutputOptionType outputOptionType = new OutputOptionType();
		outputOptionType.setOnlykeys(!detailFlag);
		outputOptionType.setBlob(blobFlag);
		outputOptionType.setTechdata(statusFlag);
		return outputOptionType;
	}

	/**
	 * Get hibernate session
	 * 
	 * @return Session
	 */
//	protected Session getSession() {
//		return HibernateUtil.getSession();
//	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public String getDbSchemaName() {
		return this.dbSchemaName;
	}

	public void setDbSchemaName(String dbSchemaName) {
		if (dbSchemaName != null && dbSchemaName.endsWith("."))
			this.dbSchemaName = dbSchemaName.trim();
		else if (dbSchemaName != null)
			this.dbSchemaName = dbSchemaName.trim() + ".";
	}

	public String handleMetaDataNumericValue(String operator, String value) {
		String formattedValue = "";
		boolean needBracesFlag = false;
		//if operator is IN, then add open and close braces if it is missing
		if (operator.toUpperCase().equals("IN")) {
			if (!SqlClauseUtil.isEnclosedinBraces(value))
				needBracesFlag = true;
		}
		if (needBracesFlag)
			formattedValue = "(" + value + ")";
		else
			formattedValue = value;
		return formattedValue;
	}

	public String handleMetaDataDateValue(String operator, String value) {
		String formattedValue = "";
		boolean needBracesFlag = false;
		//if operator is IN, then add open and close braces if it is missing
		if (operator.toUpperCase().equals("IN")) {
			if (!SqlClauseUtil.isEnclosedinBraces(value))
				needBracesFlag = true;
		}
		if (needBracesFlag)
			formattedValue = "(" + value + ")";
		else
			formattedValue = value;
		return formattedValue;
	}

	public String getOperatorByValue(String compareValue) {
		if (compareValue.startsWith("'") && compareValue.endsWith("'"))
			compareValue = compareValue.substring(1, compareValue.length() - 1);
		if (compareValue.startsWith("%") && compareValue.endsWith("%"))
			return "[";
		else if(!compareValue.startsWith("%") && compareValue.endsWith("%"))
			return "%STARTSWITH";
		return "like";
	}

	public String getCleanValue(String compareValue) {
		if (compareValue.startsWith("'") && compareValue.endsWith("'")) {
			compareValue = compareValue.substring(1, compareValue.length() - 1);
			if (compareValue.startsWith("%") && compareValue.endsWith("%"))
				compareValue = compareValue.substring(1, compareValue.length() - 1);
			else if (!compareValue.startsWith("%") && compareValue.endsWith("%"))
				compareValue = compareValue.substring(0, compareValue.length() - 1);
			return "'" + compareValue + "'";
		} else {
			if (compareValue.startsWith("%") && compareValue.endsWith("%"))
				return compareValue.substring(1, compareValue.length() - 1);
			else if (!compareValue.startsWith("%") && compareValue.endsWith("%"))
				return compareValue.substring(0, compareValue.length() - 1);
			return compareValue;
		}
	}
}
