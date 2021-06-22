/*******************************************************************************
 * Copyright (c) 2006-2018 Massachusetts General Hospital 
 * All rights reserved. This program and the accompanying materials 
 * are made available under the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. I2b2 is also distributed under
 * the terms of the Healthcare Disclaimer.
 ******************************************************************************/
/*

 * Contributors:
 *     Rajesh Kuttan
 */
package edu.harvard.i2b2.crc.dao.pdo.input;

import java.util.List;

import edu.harvard.i2b2.common.util.db.QueryUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.harvard.i2b2.common.exception.I2B2Exception;
import edu.harvard.i2b2.crc.datavo.pdo.query.ConstrainOperatorType;
import edu.harvard.i2b2.crc.datavo.pdo.query.ConstrainValueType;
import edu.harvard.i2b2.crc.datavo.pdo.query.ItemType;
import edu.harvard.i2b2.crc.util.ContainsUtil;
import edu.harvard.i2b2.crc.util.RegExUtil;
import edu.harvard.i2b2.crc.util.SqlClauseUtil;

/**
 * Class to handle value constrains. Generates sql where clause based on the
 * list of value constrains.
 * 
 * @author rkuttan
 */
public class ValueConstrainsHandler {
	/** log **/
	protected final Log log = LogFactory.getLog(getClass());

	private boolean unitCdConverstionFlag = false;
	private String unitCdInClause = StringUtils.EMPTY, unitCdSwitchClause = StringUtils.EMPTY;
	
	public void setUnitCdConversionFlag(boolean unitCdConverstionFlag, String unitCdInClause, String unitCdSwitchClause) { 
		this.unitCdConverstionFlag = unitCdConverstionFlag;
		this.unitCdInClause = unitCdInClause;
		this.unitCdSwitchClause = unitCdSwitchClause;
	}
	
	public String[] constructValueConstainClause(List<ItemType.ConstrainByValue> valueConstrainList,
												 String dbServerType,String dbSchemaName,
												 int panelAccuracyScale) throws I2B2Exception {
		String fullConstrainSql = StringUtils.EMPTY,containsJoinSql = StringUtils.EMPTY;
		System.out.println("panel accuracy scale" + panelAccuracyScale );

		for (ItemType.ConstrainByValue valueConstrain : valueConstrainList) {
			ConstrainValueType valueType = valueConstrain.getValueType();
			ConstrainOperatorType operatorType = valueConstrain.getValueOperator();
			String value = valueConstrain.getValueConstraint();
			String unitCd = valueConstrain.getValueUnitOfMeasure();
			
			String constrainSql = null;
			// check if value type is not null
			if (valueType == null)
				continue;

			if (valueType.equals(ConstrainValueType.LARGETEXT)) { 
				String containsSql;
				ContainsUtil containsUtil = new ContainsUtil();
				if (operatorType.value().equalsIgnoreCase(ConstrainOperatorType.CONTAINS.value()))
					containsSql = containsUtil.formatValue(value,dbServerType);
				else if(operatorType.value().equalsIgnoreCase(ConstrainOperatorType.CONTAINS_DATABASE.value()))
					containsSql = containsUtil.formatValue("[" + value + "]",dbServerType);
				else {
					log.debug("LARGETEXT : Invalid operator skipped [" + operatorType.value() + "]" );
					continue;
				}
					constrainSql = " valtype_cd = 'B' AND " ;
					constrainSql +=  " observation_blob " + QueryUtil.getOperatorByValue(constrainSql) +
							" '" + QueryUtil.getCleanValue(containsSql) + "' ";
			} else if (valueType.equals(ConstrainValueType.TEXT)) {
				// check if operator and value not null
				if (operatorType == null || value == null)
					continue;
				boolean notLikeFlag = false;
				if (operatorType.value().startsWith(ConstrainOperatorType.LIKE.value())) {
					//call the utility to find the like operation
					String operatorOption = RegExUtil.getOperatorOption(operatorType.value());
					if (operatorOption ==null)
						operatorOption = "[begin]";

					String likeValueFormat = StringUtils.EMPTY;
					if (operatorOption.equalsIgnoreCase("[begin]"))
						likeValueFormat = "'" + value.replaceAll("'", "''") + "%'";
					else if (operatorOption.equalsIgnoreCase("[end]"))
						likeValueFormat = "'%" + value.replaceAll("'", "''") + "'";
					else if (operatorOption.equalsIgnoreCase("[contains]"))
						likeValueFormat = "'%" + value.replaceAll("'", "''") + "%'";
					else if (operatorOption.equalsIgnoreCase("[exact]")) {
						likeValueFormat = "'" + value.replaceAll("'", "''") + "'";
						constrainSql = " obs.valtype_cd = 'T' AND obs.tval_char = " + likeValueFormat;
						notLikeFlag = true;
					}
					if (!notLikeFlag) {
						constrainSql = " obs.valtype_cd = 'T' AND upper(obs.tval_char) " +
								QueryUtil.getOperatorByValue(likeValueFormat) +
								" upper(" + QueryUtil.getCleanValue(likeValueFormat) + ")";
					}
				} else if (operatorType.value().equalsIgnoreCase(ConstrainOperatorType.EQ.value()))
					constrainSql = " obs.valtype_cd = 'T' AND obs.tval_char   = '"
							+ value.replaceAll("'", "''") + "' ";
				else if (operatorType.value().equalsIgnoreCase(ConstrainOperatorType.IN.value())) {
					value = SqlClauseUtil.buildINClause(value, true);
					constrainSql = " obs.valtype_cd = 'T' AND obs.tval_char IN (" + value + ")";
				} else if (operatorType.value().equalsIgnoreCase(ConstrainOperatorType.BETWEEN.value()))
					throw new I2B2Exception("Error in value constrain, BETWEEN operator not supported in TEXT value type [" + value + "]");
					/*
					value = SqlClauseUtil.buildBetweenClause(value);
					constrainSql = " obs.valtype_cd = 'T' AND obs.tval_char   BETWEEN "
							+ value;
					*/
					
				else if (operatorType.value().equalsIgnoreCase(ConstrainOperatorType.NE.value())) {
					String emptyStringCheck = " ";
					emptyStringCheck = " AND obs.tval_char <> '' ";
					constrainSql = " obs.valtype_cd = 'T' AND obs.tval_char   <> '"
							+ value.replaceAll("'", "''") + "' " + emptyStringCheck;
				} else {
					throw new I2B2Exception(
							"Error TEXT value constrain because operator("
									+ operatorType.toString() + ")is invalid");
				}
			} else if (valueType.equals(ConstrainValueType.NUMBER)) {
				// check if operator and value not null
				if (operatorType == null || value == null)
					continue;

				value.replaceAll("'", "''");
				
				String nvalNum = " nval_num ", unitsCdInClause = StringUtils.EMPTY;
				if (this.unitCdConverstionFlag) { 
					nvalNum = unitCdSwitchClause;
					//unitsCdInClause = this.unitCdInClause + " AND ";
					//commented not needed
//					if (unitCd != null) { 
//						unitCd = unitCd.replace("'", "''");
//						unitsCdInClause = " case when '" + unitCd + "' in " +  this.unitCdInClause + " then 1 else 0 end  =1  AND ";	
//					}
					unitsCdInClause = " ";
				}
				
				if (operatorType.value().equalsIgnoreCase(ConstrainOperatorType.GT.value()))
					constrainSql = unitsCdInClause + "  ((obs.valtype_cd = 'N' AND "+ nvalNum + " > "
							+ value
							+ " AND obs.tval_char IN ('E','GE')) OR (obs.valtype_cd = 'N' AND "+ nvalNum +" >= "
							+ value + " AND obs.tval_char = 'G' )) ";
				else if (operatorType.value().equalsIgnoreCase( ConstrainOperatorType.GE.value()))
					constrainSql =  unitsCdInClause + "  obs.valtype_cd = 'N' AND " + nvalNum + " >= "
							+ value + " AND obs.tval_char IN ('E','GE','G') ";
				else if (operatorType.value().equalsIgnoreCase( ConstrainOperatorType.LT.value()))
					constrainSql = unitsCdInClause + "  ((obs.valtype_cd = 'N' AND " + nvalNum + " < "
							+ value
							+ " AND obs.tval_char IN ('E','LE')) OR (obs.valtype_cd = 'N' AND " + nvalNum + " <= "
							+ value + " AND obs.tval_char = 'L' )) ";
				else if (operatorType.value().equalsIgnoreCase(ConstrainOperatorType.LE.value()))
					constrainSql = unitsCdInClause + "  obs.valtype_cd = 'N' AND " + nvalNum + " <= "
							+ value + " AND obs.tval_char IN ('E','LE','L') ";
				else if (operatorType.value().equalsIgnoreCase(ConstrainOperatorType.EQ.value()))
					constrainSql = unitsCdInClause + "  obs.valtype_cd = 'N' AND " + nvalNum + " = "
							+ value + " AND obs.tval_char='E' ";
				else if (operatorType.value().equalsIgnoreCase(ConstrainOperatorType.BETWEEN.value())) {
					value = SqlClauseUtil.buildBetweenClause(value);
					constrainSql = unitsCdInClause + "  obs.valtype_cd = 'N' AND " + nvalNum + " BETWEEN  "
							+ value + " AND obs.tval_char ='E' ";
				} else if (operatorType.value().equalsIgnoreCase(ConstrainOperatorType.NE.value())) {
					constrainSql = unitsCdInClause + "  ((obs.valtype_cd = 'N' AND " + nvalNum + " <> "
							+ value
							+ " AND obs.tval_char <> 'NE') OR (obs.valtype_cd = 'N' AND " + nvalNum + " = "
							+ value + " AND obs.tval_char ='NE' )) ";
				} else {
					throw new I2B2Exception(
							"Error NUMBER value constrain because operator(" + operatorType + ")is invalid");
				}
			} else if (valueType.equals(ConstrainValueType.FLAG)) {
				// check if operator and value not null
				if (operatorType == null || value == null) {
					continue;
				}
				if (operatorType.value().equalsIgnoreCase(ConstrainOperatorType.EQ.value())) {
					constrainSql = " obs.valueflag_cd = '"
							+ value.replaceAll("'", "''") + "' ";
				} else if (operatorType.value().equalsIgnoreCase(ConstrainOperatorType.NE.value())) {
					String emptyStringCheck = " ";
					emptyStringCheck = " AND obs.valueflag_cd <> '' ";
					constrainSql = "  obs.valueflag_cd <> '"
							+ value.replaceAll("'", "''") + "' " + emptyStringCheck;
				} else if (operatorType.value().equalsIgnoreCase(ConstrainOperatorType.IN.value())) {
					value = SqlClauseUtil.buildINClause(value, true);
					constrainSql = " obs.valueflag_cd IN (" + value +")";
				} else {
					throw new I2B2Exception(
							"Error FLAG value constrain because operator("
									+ operatorType.toString() + ")is invalid");
				}
			} else if (valueType.equals(ConstrainValueType.MODIFIER)) {
				// check if operator and value not null
				if (operatorType == null || value == null)
					continue;

				if (value != null) {
					if (operatorType.value().equalsIgnoreCase(ConstrainOperatorType.EQ.value()))
						constrainSql = " obs.valtype_cd = 'M' and obs.tval_char = '"
								+ value.replaceAll("'", "''") + "' ";
					else if (operatorType.value().equalsIgnoreCase(ConstrainOperatorType.NE.value()))
						constrainSql = " obs.valtype_cd = 'M' and obs.tval_char <> '"
								+ value.replaceAll("'", "''") + "' AND tval_char <> ''";
					else if (operatorType.value().equalsIgnoreCase(ConstrainOperatorType.IN.value())) {
						value = SqlClauseUtil.buildINClause(value, true);
						constrainSql = " obs.valtype_cd = 'M' and obs.tval_char IN (" + value + ") ";
					}
				}
			} else
				throw new I2B2Exception(
						"Error value constrain, invalid value type (" + valueType.toString() + ")");
			if (constrainSql != null) {
				if (fullConstrainSql.length() > 0)
					fullConstrainSql += " AND ";
				fullConstrainSql += constrainSql;
			}
		}
		return new String[] { fullConstrainSql, containsJoinSql};
	}
}
