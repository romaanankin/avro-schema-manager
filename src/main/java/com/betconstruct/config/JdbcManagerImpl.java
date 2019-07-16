package com.betconstruct.config;

import org.relique.jdbc.csv.CsvDriver;

import javax.sql.DataSource;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class JdbcManagerImpl implements JdbcManager {
    private final DataSource dataSource;
    private final PreparedStatementParameterSetter preparedStatementParameterSetter;

    public JdbcManagerImpl(DataSource dataSource, PreparedStatementParameterSetter preparedStatementParameterSetter) {
        this.dataSource = dataSource;
        this.preparedStatementParameterSetter = preparedStatementParameterSetter;
    }

    protected Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            System.out.println(e.getSQLState());
            throw new RuntimeException();
        }
    }

    private void closeQuietly(Connection connection,
                              PreparedStatement statement,
                              ResultSet resultSet) {
        if (null != resultSet)
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        if (null != statement)
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        if (null != connection)
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
    }

    @Override
    public void writeCsv(String sql, PrintStream printStream, Object... parameters) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection();
            statement = connection.prepareStatement(sql);
            preparedStatementParameterSetter.setParameters(statement, parameters);
            resultSet = statement.executeQuery();
            CsvDriver.writeToCsv(resultSet, printStream, true);
        } catch (SQLException e) {
            e.getErrorCode();
        } finally {
            closeQuietly(connection, statement, resultSet);
        }
    }

    @Override
    public <T> List<T> select(String sql, RowMapper<T> rowMapper, Object... parameters) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        final List<T> result = new ArrayList<>();
        try {
            connection = getConnection();
            statement = connection.prepareStatement(sql);
            preparedStatementParameterSetter.setParameters(statement, parameters);
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                result.add(rowMapper.map(resultSet));
            }
        } catch (final SQLException e) {
            e.getErrorCode();
        } finally {
            closeQuietly(connection, statement, resultSet);
        }
        return result;
    }

    @Override
    public long insertAndGetId(String sql, Object... parameters) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            preparedStatementParameterSetter.setParameters(statement, parameters);
            final int result = statement.executeUpdate();
            Long id = null;
            if (0 != result) {
                resultSet = statement.getGeneratedKeys();
                if (resultSet.next()) {
                    id = resultSet.getLong(1);
                }
            }
            if (null == id) {
                throw new SQLException("No id is returned");
            }
            connection.commit();
            return id;
        } catch (SQLException e) {
            rollback(connection);
            throw e;
        } catch (final Exception e) {
            rollback(connection);
            throw new SQLException(e);
        } finally {
            closeQuietly(connection, statement, resultSet);
        }
    }

    @Override
    public void insert(String sql, Object... parameters) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(sql);
            preparedStatementParameterSetter.setParameters(statement, parameters);
            statement.executeUpdate();
            connection.commit();
        } catch (final SQLException e) {
            rollback(connection);
            throw e;
        } catch (final Exception e) {
            rollback(connection);
            throw new SQLException(e);
        } finally {
            closeQuietly(connection, statement, resultSet);
        }
    }

    @Override
    public int update(String sql, Object... parameters) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        final ResultSet resultSet = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(sql);
            preparedStatementParameterSetter.setParameters(statement, parameters);
            final int result = statement.executeUpdate();
            connection.commit();
            return result;
        } catch (final SQLException e) {
            rollback(connection);
            throw e;
        } catch (Exception e) {
            rollback(connection);
            throw new SQLException(e);
        } finally {
            closeQuietly(connection, statement, resultSet);
        }
    }

    private void rollback(Connection connection) {
        if (null != connection) {
            try {
                connection.rollback();
            } catch (final SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
