package us.dot.faa.swim.fns.notamdb;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.DbUtils;
import org.json.XML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import us.dot.faa.swim.fns.FnsMessage;

public class NotamDb {
	private final static Logger logger = LoggerFactory.getLogger(NotamDb.class);
	private final NotamDbConfig config;
	private boolean isValid = false;
	private boolean isInitializing = false;

	private final BasicDataSource notamDbDataSource = new BasicDataSource();

	public NotamDb(NotamDbConfig config) throws Exception {
		this.config = config;

		if (!config.getDriver().equals("org.postgresql.Driver")) {
			throw new Exception("DB Driver: " + config.getDriver()
					+ " currently not supported. Only postgresql is supported.");
		}

		notamDbDataSource.setDriverClassName(config.getDriver());
		notamDbDataSource.setUrl(config.getConnectionUrl());
		notamDbDataSource.setUsername(config.getUsername());
		notamDbDataSource.setPassword(config.getPassword());
		notamDbDataSource.setMinIdle(0);
		notamDbDataSource.setMaxIdle(10);
		notamDbDataSource.setMaxOpenPreparedStatements(100);
	}

	public boolean isValid() {
		return this.isValid;
	}

	public void setValid() {
		this.isValid = true;
	}

	public void setInvalid() {
		this.isValid = false;
	}

	public boolean isInitializing() {
		return this.isInitializing;
	}

	public void setInitializing(boolean isInitalizing) {
		this.isInitializing = isInitalizing;
	}

	public NotamDbConfig getConfig() {
		return this.config;
	}

	public boolean notamTableExists() throws SQLException {
		Connection conn = null;
		ResultSet rset = null;
		try {
			conn = getDBConnection();
			rset = conn.getMetaData().getTables(null, this.config.schema, this.config.table, null);
			if (rset.next()) {
				return true;
			}
		} finally {
			DbUtils.closeQuietly(rset);
			DbUtils.closeQuietly(conn);
		}
		return false;
	}

	public AbstractMap.SimpleEntry<Long, Instant> getLastCorrelationId() throws SQLException {
		Connection conn = null;
		PreparedStatement getLastCorrelationIdPreparedStatement = null;
		try {
			conn = getDBConnection();
			getLastCorrelationIdPreparedStatement = conn.prepareStatement(
					"SELECT storedTimeStamp, correlationid FROM NOTAMS ORDER BY correlationid DESC LIMIT 1");
			ResultSet rs = getLastCorrelationIdPreparedStatement.executeQuery();
			if (rs.next()) {
				return new AbstractMap.SimpleEntry<>(rs.getLong("correlationid"),
						rs.getTimestamp("storedTimeStamp").toInstant());
			} else {
				return null;
			}
		} finally {
			DbUtils.closeQuietly(getLastCorrelationIdPreparedStatement);
			DbUtils.closeQuietly(conn);
		}
	}

	public void dropNotamTable() throws SQLException {
		Connection conn = null;
		try {
			conn = getDBConnection();
			if (notamTableExists()) {
				logger.info("Dropping NOTAMS Table");
				final String dropQuery = "DROP TABLE " + this.config.table;
				conn.prepareStatement(dropQuery).execute();
			}
		} finally {
			DbUtils.closeQuietly(conn);
		}
	}

	public void createNotamTable() throws SQLException {
		Connection conn = getDBConnection();
		try {
			logger.info("Creating new NOTAMS Table");
			final String createQuery = "CREATE TABLE " + this.config.table + "(fnsid int primary key, "
					+ "correlationId bigint, issuedTimestamp timestamp, storedTimeStamp timestamp, "
					+ "updatedTimestamp timestamp, validFromTimestamp timestamp, validToTimestamp timestamp, "
					+ "classification varchar(4), locationDesignator varchar(12), notamAccountability varchar(12), "
					+ "notamText text, aixmNotamMessage xml, status varchar(12), "
					+ "validToEstimated varchar(1), notamId varchar(12), xoverNotamId varchar(12), "
					+ "xoverNotamAccountability varchar(12), icaoLocation varchar(12))";
			conn.prepareStatement(createQuery).execute();
		} finally {
			DbUtils.closeQuietly(conn);
		}
	}

	public void putNotam(final FnsMessage fnsMessage) throws SQLException {
		Connection conn = null;
		try {
			conn = getDBConnection();
			putNotam(conn, fnsMessage);
		} catch (SQLException e) {
			isValid = false;
			throw e;
		} finally {
			DbUtils.closeQuietly(conn);
		}
	}

	public void putNotam(final Connection conn, final FnsMessage fnsMessage) throws SQLException {
		PreparedStatement putNotamPreparedStatement = null;
		try {
			if (!this.isInitializing && !checkIfNotamIsNewer(fnsMessage)) {
				logger.debug("NOTAM with FNS_ID:" + fnsMessage.getFNS_ID() + " and CorrelationId: "
						+ fnsMessage.getCorrelationId() + " and LastUpdateTime: "
						+ fnsMessage.getUpdatedTimestamp().toString()
						+ " discarded due to Notam in database has newer LastUpdateTime");
				return;
			}

			putNotamPreparedStatement = createPutNotamPreparedStatement(conn);
			populatePutNotamPreparedStatement(putNotamPreparedStatement, fnsMessage);
			putNotamPreparedStatement.executeUpdate();
		} finally {
			DbUtils.closeQuietly((putNotamPreparedStatement));
		}
	}

	private PreparedStatement createPutNotamPreparedStatement(Connection conn) throws SQLException {

		String putNotamSql = "INSERT INTO " + this.config.table
				+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
				+ "ON CONFLICT (fnsid) DO UPDATE SET "
				+ "correlationId=EXCLUDED.correlationId, updatedTimestamp=EXCLUDED.updatedTimestamp, "
				+ "validFromTimestamp=EXCLUDED.validFromTimestamp, validToTimestamp=EXCLUDED.validToTimestamp, "
				+ "classification=EXCLUDED.classification, locationDesignator=EXCLUDED.locationDesignator, "
				+ "notamAccountability=EXCLUDED.notamAccountability, notamText=EXCLUDED.notamText, "
				+ "aixmNotamMessage=EXCLUDED.aixmNotamMessage, status=EXCLUDED.status, "
				+ "validToEstimated=EXCLUDED.validToEstimated, notamId=EXCLUDED.notamId, "
				+ "xoverNotamId=EXCLUDED.xoverNotamId, xoverNotamAccountability=EXCLUDED.xoverNotamAccountability, "
				+ "icaoLocation=EXCLUDED.icaoLocation";

		return conn.prepareStatement(putNotamSql);
	}

	private void populatePutNotamPreparedStatement(PreparedStatement putNotamPreparedStatement,
			final FnsMessage fnsMessage) throws SQLException {
		putNotamPreparedStatement.setLong(1, fnsMessage.getFNS_ID());
		putNotamPreparedStatement.setLong(2, fnsMessage.getCorrelationId());
		putNotamPreparedStatement.setTimestamp(3, fnsMessage.getIssuedTimestamp());
		putNotamPreparedStatement.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
		putNotamPreparedStatement.setTimestamp(5, fnsMessage.getUpdatedTimestamp());
		putNotamPreparedStatement.setTimestamp(6, fnsMessage.getValidFromTimestamp());
		putNotamPreparedStatement.setTimestamp(7, fnsMessage.getValidToTimestamp());
		putNotamPreparedStatement.setString(8, fnsMessage.getClassification());
		putNotamPreparedStatement.setString(9, fnsMessage.getLocationDesignator());
		putNotamPreparedStatement.setString(10, fnsMessage.getNotamAccountability());
		putNotamPreparedStatement.setString(11, fnsMessage.getNotamText());

		SQLXML aixmNotamMessageSqlXml = putNotamPreparedStatement.getConnection().createSQLXML();
		aixmNotamMessageSqlXml.setString(fnsMessage.getAixmNotamMessage());
		putNotamPreparedStatement.setSQLXML(12, aixmNotamMessageSqlXml);

		putNotamPreparedStatement.setString(13, fnsMessage.getStatus().toString());
		putNotamPreparedStatement.setString(14, fnsMessage.getValidToEstimated()? "Y" : "N");
		putNotamPreparedStatement.setString(15, fnsMessage.getNotamId());
		putNotamPreparedStatement.setString(16, fnsMessage.getXoverNotamId());
		putNotamPreparedStatement.setString(17, fnsMessage.getXoverNotamAccountability());
		putNotamPreparedStatement.setString(18, fnsMessage.getIcaoLocation());
	}

	public boolean checkIfNotamIsNewer(final FnsMessage fnsMessage) throws SQLException {

		Connection conn = getDBConnection();
		PreparedStatement checkIfNotamIsNewerPreparedStatement = null;
		try {
			logger.debug("Looking up up if NOTAM with FNS_ID:" + fnsMessage.getFNS_ID() + " and CorrelationId: "
					+ fnsMessage.getCorrelationId() + " and LastUpdateTime: "
					+ fnsMessage.getUpdatedTimestamp().toString());

			checkIfNotamIsNewerPreparedStatement = conn
					.prepareStatement("SELECT updatedtimestamp FROM NOTAMS WHERE fnsid=?");
			checkIfNotamIsNewerPreparedStatement.setLong(1, fnsMessage.getFNS_ID());

			ResultSet rset = checkIfNotamIsNewerPreparedStatement.executeQuery();

			if (!rset.next()) {
				return true;
			} else if (rset.getTimestamp("updatedtimestamp").getTime() < fnsMessage.getUpdatedTimestamp().toInstant()
					.toEpochMilli()) {
				return true;
			}
		} finally {
			DbUtils.closeQuietly(checkIfNotamIsNewerPreparedStatement);
			DbUtils.closeQuietly(conn);
		}
		return false;
	}

	public int removeOldNotams() throws SQLException {
		Connection conn = getDBConnection();
		PreparedStatement putMessagePreparedStatement;
		try {
			putMessagePreparedStatement = conn.prepareStatement(
					"DELETE FROM NOTAMS WHERE validtotimestamp AT TIME ZONE 'UTC' < NOW()");
			int recordsDeleted = putMessagePreparedStatement.executeUpdate();
			putMessagePreparedStatement.close();

			putMessagePreparedStatement = conn.prepareStatement("DELETE FROM NOTAMS WHERE status != 'ACTIVE'");
			recordsDeleted = recordsDeleted + putMessagePreparedStatement.executeUpdate();
			putMessagePreparedStatement.close();

			return recordsDeleted;
		} finally {
			DbUtils.closeQuietly(conn);
		}
	}

	public Connection getDBConnection() throws SQLException {
		return notamDbDataSource.getConnection();
	}

	// db lookups
	public void getByLocationDesignator(String locationDesignator, OutputStream output, boolean asJson)
			throws SQLException, IOException {

		Connection conn = getDBConnection();
		PreparedStatement selectPreparedStatement = null;
		try {
			selectPreparedStatement = conn.prepareStatement(
					"select fnsid, aixmNotamMessage from " + this.config.table + " where locationDesignator = ?"
							+ " AND status = 'ACTIVE' AND (validtotimestamp > NOW() OR validtotimestamp is null)");
			selectPreparedStatement.setString(1, locationDesignator);

			writeResponseToSteam(selectPreparedStatement, output, asJson);
		} catch (SQLException e) {
			logger.error("Createing Select Statement: " + e.getMessage());
		} finally {
			DbUtils.closeQuietly(selectPreparedStatement);
			DbUtils.closeQuietly(conn);
		}
	}

	public void getByClassification(String classification, OutputStream output, boolean asJson)
			throws SQLException, IOException {

		Connection conn = getDBConnection();
		PreparedStatement selectPreparedStatement = null;
		try {
			selectPreparedStatement = conn.prepareStatement("SELECT fnsid, aixmNotamMessage FROM " + this.config.table
					+ " WHERE classification = ? AND status = 'ACTIVE' AND (validtotimestamp > NOW() OR validtotimestamp is null)");
			selectPreparedStatement.setString(1, classification);

			writeResponseToSteam(selectPreparedStatement, output, asJson);
		} catch (SQLException e) {
			logger.error("Createing Select Statement: " + e.getMessage());
		} finally {
			DbUtils.closeQuietly(selectPreparedStatement);
			DbUtils.closeQuietly(conn);
		}
	}

	public void getDelta(String deltaTime, OutputStream output, boolean asJson)
			throws SQLException, IOException {
		Connection conn = getDBConnection();
		PreparedStatement selectPreparedStatement = null;
		try {
			selectPreparedStatement = conn.prepareStatement("SELECT fnsid, aixmNotamMessage FROM " + this.config.table
					+ " WHERE updatedTimestamp >= ? OR validtotimestamp is null");
			selectPreparedStatement.setTimestamp(1, Timestamp.valueOf(deltaTime));

			writeResponseToSteam(selectPreparedStatement, output, asJson);
		} catch (SQLException e) {
			logger.error("[DB] Error Createing Select Statement: " + e.getMessage());
		} finally {
			DbUtils.closeQuietly(selectPreparedStatement);
			DbUtils.closeQuietly(conn);
		}
	}

	public void getByTimeRange(String fromDateTime, String toDateTime, OutputStream output, boolean asJson)
			throws SQLException, IOException {
		Connection conn = getDBConnection();
		PreparedStatement selectPreparedStatement = null;
		try {
			selectPreparedStatement = conn.prepareStatement("SELECT fnsid, aixmNotamMessage from " + this.config.table
					+ " WHERE validFromTimestamp >= ? AND (validToTimestamp <= ? OR validToTimestamp is null) AND status = 'ACTIVE'");

			selectPreparedStatement.setTimestamp(1, Timestamp.valueOf(fromDateTime));
			selectPreparedStatement.setTimestamp(2, Timestamp.valueOf(toDateTime));

			writeResponseToSteam(selectPreparedStatement, output, asJson);
		} catch (SQLException e) {
			logger.error("Createing Select Statement: " + e.getMessage());
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(e);
		} finally {
			DbUtils.closeQuietly(selectPreparedStatement);
			DbUtils.closeQuietly(conn);
		}
	}

	public void getAllNotams(OutputStream output, boolean asJson) throws SQLException, IOException {
		final Connection conn = getDBConnection();
		PreparedStatement selectPreparedStatement = null;
		try {
			selectPreparedStatement = conn.prepareStatement("SELECT aixmNotamMessage FROM " + this.config.table
					+ " WHERE status = 'ACTIVE' AND (validtotimestamp > NOW()  OR validtotimestamp is null)");

			writeResponseToSteam(selectPreparedStatement, output, asJson);
		} catch (SQLException e) {
			logger.error("[DB] Error Createing Select Statement: " + e.getMessage());
		} finally {
			DbUtils.closeQuietly(selectPreparedStatement);
			DbUtils.closeQuietly(conn);
		}
	}

	private void writeResponseToSteam(PreparedStatement selectPreparedStatement, OutputStream output, boolean asJson)
			throws SQLException, IOException {
		final ResultSet resultSet = selectPreparedStatement.executeQuery();

		OutputStream bos = new BufferedOutputStream(output);
		if (asJson) {
			bos.write("{\n\"AixmBasicMessageCollection\": [\n".getBytes());
		} else {
			bos.write("<AixmBasicMessageCollection>".getBytes());
		}

		boolean first = true;
		while (resultSet.next()) {
			if (asJson) {
				if (!first) {
					bos.write(",\n".getBytes());
				}
				first = false;
				bos.write(XML.toJSONObject(resultSet.getString("aixmNotamMessage")).toString().getBytes());
			} else {
				bos.write(resultSet.getString("aixmNotamMessage").replaceAll("<\\?xml(.+?)\\?>", "").trim().getBytes());
			}
		}

		if (asJson) {
			bos.write("\n]\n}".getBytes());
		} else {
			bos.write("</AixmBasicMessageCollection>".getBytes());
		}

		bos.flush();
		bos.close();
	}

	public Map<String, Timestamp> getValidationMap() throws SQLException {

		Connection conn = getDBConnection();
		PreparedStatement selectPreparedStatement = null;
		try {
			selectPreparedStatement = conn.prepareStatement("select fnsid, updatedtimestamp from " + this.config.table);

			return createValidationMapFromDatabase(selectPreparedStatement);

		} finally {
			DbUtils.closeQuietly(selectPreparedStatement);
			DbUtils.closeQuietly(conn);
		}
	}

	private Map<String, Timestamp> createValidationMapFromDatabase(PreparedStatement selectPreparedStatement)
			throws SQLException {
		Map<String, Timestamp> validationMap = new HashMap<>();

		final ResultSet resultSet = selectPreparedStatement.executeQuery();

		while (resultSet.next()) {

			String fnsId = resultSet.getString("fnsid");
			Timestamp updatedTimestamp = resultSet.getTimestamp("updatedTimestamp");

			validationMap.put(fnsId, updatedTimestamp);

		}

		return validationMap;
	}

}
