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

import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import edu.harvard.i2b2.common.exception.I2B2DAOException;
import edu.harvard.i2b2.common.util.jaxb.JAXBUtil;
import edu.harvard.i2b2.common.util.jaxb.JAXBUtilException;
import edu.harvard.i2b2.crc.dao.CRCDAO;
import edu.harvard.i2b2.crc.dao.DAOFactoryHelper;
import edu.harvard.i2b2.crc.datavo.CRCJAXBUtil;
import edu.harvard.i2b2.crc.datavo.db.DataSourceLookup;
import edu.harvard.i2b2.crc.datavo.db.QtQueryInstance;
import edu.harvard.i2b2.crc.datavo.db.QtQueryResultInstance;
import edu.harvard.i2b2.crc.datavo.db.QtXmlResult;
import edu.harvard.i2b2.crc.datavo.i2b2result.BodyType;
import edu.harvard.i2b2.crc.datavo.i2b2result.DataType;
import edu.harvard.i2b2.crc.datavo.i2b2result.ResultEnvelopeType;
import edu.harvard.i2b2.crc.datavo.i2b2result.ResultType;

/**
 * Class to handle persistance operation of Query instance i.e. each run of
 * query is called query instance $Id: QueryInstanceSpringDao.java,v 1.4
 * 2008/04/08 19:38:24 rk903 Exp $
 * 
 * @author rkuttan
 * @see QtQueryInstance
 */
public class XmlResultSpringDao extends CRCDAO implements IXmlResultDao  {

	JdbcTemplate jdbcTemplate = null;

	QtXmlResultRowMapper xmlResultMapper = new QtXmlResultRowMapper();
	private DataSourceLookup dataSourceLookup = null;

	public XmlResultSpringDao(DataSource dataSource,
			DataSourceLookup dataSourceLookup) {
		setDataSource(dataSource);
		setDbSchemaName(dataSourceLookup.getFullSchema());
		jdbcTemplate = new JdbcTemplate(dataSource);
		this.dataSourceLookup = dataSourceLookup;

	}

	/**
	 * Function to create query instance
	 * 
	 * @param resultInstanceId
	 * @param xmlValue
	 * @return query instance id
	 */
	@Override
	public String createQueryXmlResult(String resultInstanceId, String xmlValue) {
		String sql = "INSERT INTO " + getDbSchemaName() + "QT_XML_RESULT(xml_result_id,result_instance_id,xml_value) VALUES(?,?,?)";
		String sequence = "select I2B2.Utils_nextval('qt_xml_result_xml_result_id_seq')";
		int xmlResultId = jdbcTemplate.queryForObject(sequence, Integer.class);
		jdbcTemplate.update(sql, new Object[]{xmlResultId, Integer.parseInt(resultInstanceId), xmlValue});
		return String.valueOf(xmlResultId);
	}

	/**
	 * Returns list of query instance for the given master id
	 * 
	 * @param resultInstanceId
	 * @return List<QtQueryInstance>
	 */
	@Override
	@SuppressWarnings("unchecked")
	public QtXmlResult getXmlResultByResultInstanceId(String resultInstanceId) throws I2B2DAOException {
		String sql = "select *  from " + getDbSchemaName() + "qt_xml_result where result_instance_id = ?";
		List<QtXmlResult> queryXmlResult = jdbcTemplate.query(sql,
				new Object[] { Integer.parseInt(resultInstanceId) }, xmlResultMapper);
		if (queryXmlResult != null && queryXmlResult.size() > 0)
			return queryXmlResult.get(0);
		else {
			//Check to see if it is being worked on
			sql = "select b.name from " + getDbSchemaName()
					+ "qt_query_result_instance a, " + getDbSchemaName()
					+ "qt_query_result_type b where a.result_instance_id = ? and a.result_type_id = b.result_type_id";

			String resultName = null;
			try {
				resultName = jdbcTemplate.queryForObject(sql,
						new Object[] { Integer.parseInt(resultInstanceId) }, String.class);
			} catch (Exception e) {
			}

			if (resultName != null) {
				ResultType resultType = new ResultType();
				resultType.setName(resultName);
				DataType mdataType = new DataType();
				mdataType.setValue("-1");
				mdataType.setColumn("Processing");
				mdataType.setType("int");
				resultType.getData().add(mdataType);

				edu.harvard.i2b2.crc.datavo.i2b2result.ObjectFactory of = new edu.harvard.i2b2.crc.datavo.i2b2result.ObjectFactory();
				BodyType bodyType = new BodyType();
				bodyType.getAny().add(of.createResult(resultType));
				ResultEnvelopeType resultEnvelop = new ResultEnvelopeType();
				resultEnvelop.setBody(bodyType);

				JAXBUtil jaxbUtil = CRCJAXBUtil.getJAXBUtil();
				StringWriter strWriter = new StringWriter();
				try {
					jaxbUtil.marshaller(of.createI2B2ResultEnvelope(resultEnvelop), strWriter);
				} catch (JAXBUtilException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				QtXmlResult xmlResult = new QtXmlResult();
				xmlResult.setXmlResultId(resultInstanceId);
				QtQueryResultInstance queryResultInstance = new QtQueryResultInstance();
				queryResultInstance.setResultInstanceId(resultInstanceId);
				xmlResult.setQtQueryResultInstance(queryResultInstance);
				xmlResult.setXmlValue(strWriter.toString());
				return xmlResult;
			} else {
				throw new I2B2DAOException("Query result instance  id " + resultInstanceId
						+ " not found");
			}
		}
	}

	private static class QtXmlResultRowMapper implements RowMapper {
		@Override
		public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
			QtXmlResult xmlResult = new QtXmlResult();
			xmlResult.setXmlResultId(rs.getString("XML_RESULT_ID"));
			QtQueryResultInstance queryResultInstance = new QtQueryResultInstance();
			queryResultInstance.setResultInstanceId(rs.getString("RESULT_INSTANCE_ID"));
			xmlResult.setQtQueryResultInstance(queryResultInstance);
			xmlResult.setXmlValue(rs.getString("XML_VALUE"));
			return xmlResult;
		}
	}
}
