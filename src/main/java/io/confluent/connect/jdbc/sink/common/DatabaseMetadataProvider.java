package io.confluent.connect.jdbc.sink.common;

import io.confluent.connect.jdbc.sink.ConnectionProvider;

import java.sql.SQLException;

public interface DatabaseMetadataProvider {
  DatabaseMetadata get(final ConnectionProvider connectionProvider) throws SQLException;
}
