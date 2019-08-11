package com.config;

import java.io.PrintStream;
import java.sql.SQLException;
import java.util.List;

public interface JdbcManager {
    <T> List<T> select(String sql, RowMapper<T> rowMapper, Object... parameters) throws SQLException;

    int update(final String sql, final Object... parameters) throws SQLException;

    long insertAndGetId(final String sql, final Object... parameters) throws SQLException;

    void insert(final String sql, final Object... parameters) throws SQLException;

    void writeCsv(final String sql, PrintStream printStream, final Object... parameters) throws SQLException;
}
