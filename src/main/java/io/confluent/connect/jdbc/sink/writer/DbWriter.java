package io.confluent.connect.jdbc.sink.writer;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;

/**
 * Defines the contract for inserting a new entry fromthe connect sink record
 */
public interface DbWriter extends AutoCloseable {
  void write(Collection<SinkRecord> records);
}
