package com.betconstruct.config;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

import java.util.Properties;

import static com.betconstruct.AvroManager.DB_NAME;
import static com.betconstruct.AvroManager.DB_URL;
import static com.betconstruct.AvroManager.PASSWORD;
import static com.betconstruct.AvroManager.USER;

public class DBUtil {
    public static JdbcManager getJdbcManager(Properties properties) {
        PreparedStatementParameterSetter statementSetter = new DefaultPreparedStatementParameterSetter();
        SQLServerDataSource dataSource = new SQLServerDataSource();
        dataSource.setURL(properties.getProperty(DB_URL));
        dataSource.setDatabaseName(properties.getProperty(DB_NAME));
        dataSource.setUser(properties.getProperty(USER));
        dataSource.setPassword(properties.getProperty(PASSWORD));
        return new JdbcManagerImpl(dataSource, statementSetter);
    }
}
