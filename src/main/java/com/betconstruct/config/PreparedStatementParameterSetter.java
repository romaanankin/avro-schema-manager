package com.betconstruct.config;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface PreparedStatementParameterSetter {
    void setParameters(final PreparedStatement statement, final Object... parameters) throws SQLException;

}
