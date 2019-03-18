package com.blueapron.connect.protobuf;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class ProtobufConverterNullTest {
  private final String TEST_MESSAGE_CLASS_NAME = "com.blueapron.connect.protobuf.NestedTestProtoOuterClass$NestedTestProto";

  private Schema getMessageSchema() {
    final SchemaBuilder builder = SchemaBuilder.struct();
    final SchemaBuilder userIdBuilder = SchemaBuilder.struct();
    userIdBuilder.field("ba_com_user_id", SchemaBuilder.string().optional().build());
    userIdBuilder.field("other_user_id", SchemaBuilder.int32().optional().build());
    final SchemaBuilder messageIdBuilder = SchemaBuilder.struct();
    messageIdBuilder.field("id", SchemaBuilder.string().optional().build());
    userIdBuilder.field("another_id", messageIdBuilder.optional().build());
    builder.field("user_id", userIdBuilder.optional().build());
    builder.field("is_active", SchemaBuilder.bool().optional().build());
    builder.field("experiments_active", SchemaBuilder.array(SchemaBuilder.string().optional().build()).optional().build());
    builder.field("updated_at", org.apache.kafka.connect.data.Timestamp.builder().optional().build());
    builder.field("status", SchemaBuilder.string().optional().build());

    final SchemaBuilder complexTypeBuilder = SchemaBuilder.struct();
    complexTypeBuilder.field("one_id", SchemaBuilder.string().optional().build());
    complexTypeBuilder.field("other_id", SchemaBuilder.int32().optional().build());
    complexTypeBuilder.field("is_active", SchemaBuilder.bool().optional().build());
    builder.field("complex_type", complexTypeBuilder.optional().build());

    builder.field("map_type", SchemaBuilder.array(SchemaBuilder.struct().field("key", Schema.OPTIONAL_STRING_SCHEMA).field("value", Schema.OPTIONAL_STRING_SCHEMA).optional().build()).optional().build());
    return builder.build();
  }

    private Struct getMessageValue() {
    Schema schema = getMessageSchema();
    Struct result = new Struct(schema.schema());

    Struct userId = new Struct(schema.field("user_id").schema());
    userId.put("ba_com_user_id", null);
    userId.put("other_user_id", null);
    Struct anotherId = new Struct(schema.field("user_id").schema().field("another_id").schema());
    anotherId.put("id", null);
    userId.put("another_id", anotherId);
    result.put("user_id", userId);

    result.put("is_active", null);
    result.put("experiments_active", null);
    result.put("updated_at", null);
    result.put("status", null);

    Struct complextType = new Struct(schema.field("complex_type").schema());
    complextType.put("one_id", null);
    complextType.put("other_id", null);
    complextType.put("is_active", null);
    result.put("complex_type", complextType);

    result.put("map_type", null);
    return result;
  }

  private Schema getNestedMessageSchema() {
    final SchemaBuilder builder = SchemaBuilder.struct();
    builder.field("user_id.ba_com_user_id", SchemaBuilder.string().optional().build());
    builder.field("user_id.other_user_id", SchemaBuilder.int32().optional().build());
    builder.field("user_id.another_id.id", SchemaBuilder.string().optional().build());
    builder.field("is_active", SchemaBuilder.bool().optional().build());
    builder.field("experiments_active", SchemaBuilder.array(SchemaBuilder.string().optional().build()).optional().build());
    builder.field("updated_at", org.apache.kafka.connect.data.Timestamp.builder().optional().build());
    builder.field("status", SchemaBuilder.string().optional().build());
    builder.field("complex_type.one_id", SchemaBuilder.string().optional().build());
    builder.field("complex_type.other_id", SchemaBuilder.int32().optional().build());
    builder.field("complex_type.is_active", SchemaBuilder.bool().optional().build());
    builder.field("map_type", SchemaBuilder.array(SchemaBuilder.struct().field("key", Schema.OPTIONAL_STRING_SCHEMA).field("value", Schema.OPTIONAL_STRING_SCHEMA).optional().build()).optional().build());
    return builder.build();
  }

  private Struct getNestedMessageValue() {
    Schema schema = getNestedMessageSchema();
    Struct result = new Struct(schema.schema());
    result.put("user_id.another_id.id", null);
    result.put("is_active", null);
    result.put("experiments_active", null);
    result.put("updated_at", null);
    result.put("status", null);
    result.put("complex_type.one_id", null);
    result.put("complex_type.is_active", null);
    result.put("map_type", null);
    return result;
  }

  private ProtobufConverter getConfiguredProtobufConverter(String protobufClassName, boolean flatConnectSchema, boolean propagateNullValue, boolean isKey) {
    ProtobufConverter protobufConverter = new ProtobufConverter();

    Map<String, Object> configs = new HashMap<String, Object>();
    configs.put("protoClassName", protobufClassName);
    configs.put("flatConnectSchema", flatConnectSchema);
    configs.put("propagateNullValue", propagateNullValue);

    protobufConverter.configure(configs, isKey);

    return protobufConverter;
  }

  @Test
  public void testToConnectDataForNullValue() {
    ProtobufConverter testMessageConverter = getConfiguredProtobufConverter(TEST_MESSAGE_CLASS_NAME, false, true, false);
    SchemaAndValue result = testMessageConverter.toConnectData("my-topic", null);
    SchemaAndValue expected = new SchemaAndValue(getMessageSchema(), getMessageValue());

    assertEquals(expected, result);
  }

  @Test
  public void testToConnectDataForNestedNullValue() {
    ProtobufConverter testMessageConverter = getConfiguredProtobufConverter(TEST_MESSAGE_CLASS_NAME, true, true, false);
    SchemaAndValue result = testMessageConverter.toConnectData("my-topic", null);
    SchemaAndValue expected = new SchemaAndValue(getNestedMessageSchema(), getNestedMessageValue());

    assertEquals(expected, result);
  }
}
