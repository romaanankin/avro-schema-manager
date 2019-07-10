package com.betconstruct.config;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

import java.util.Properties;

public class DBUtil {
    public static JdbcManager getJdbcManager(Properties properties) {
        PreparedStatementParameterSetter statementSetter = new DefaultPreparedStatementParameterSetter();
        SQLServerDataSource dataSource = new SQLServerDataSource();
        dataSource.setURL(properties.getProperty(AvroManagerConfig.DB_URL));
        dataSource.setDatabaseName(properties.getProperty(AvroManagerConfig.DB_NAME));
        dataSource.setUser(properties.getProperty(AvroManagerConfig.USER));
        dataSource.setPassword(properties.getProperty(AvroManagerConfig.PASSWORD));
        return new JdbcManagerImpl(dataSource, statementSetter);
    }
}
