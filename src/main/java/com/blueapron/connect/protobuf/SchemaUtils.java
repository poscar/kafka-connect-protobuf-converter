package com.blueapron.connect.protobuf;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javafx.util.Pair;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class SchemaUtils {
  static SchemaAndValue flattenNestedStructs(SchemaAndValue connectData, String fieldDelimiter) {
    Schema origSchema = connectData.schema();
    Struct origValue = (Struct) connectData.value();

    Pair<Schema, Map<String, Object>> flatSchemaAndValuesMap = createFlatSchemaAndValuesMap(origSchema, origValue, fieldDelimiter);

    Schema flatSchema = flatSchemaAndValuesMap.getKey();
    Map<String, Object> flatValuesMap = flatSchemaAndValuesMap.getValue();
    Struct flatStruct = createPopulatedFlatStruct(flatSchema, flatValuesMap);
    return new SchemaAndValue(flatSchema, flatStruct);
  }

  private static Pair<Schema, Map<String, Object>> createFlatSchemaAndValuesMap(Schema origSchema, Struct origValue, String fieldDelimiter) {
    SchemaBuilder flatSchemaBuilder = SchemaBuilder.struct();
    Map<String, Object> flatValuesMap = new HashMap<>();
    String fieldNamePrefix = "";

    // Recursively traverse the original schema and value struct populating our flat schema and
    // values map.
    populateFlatSchemaAndValuesMap(flatSchemaBuilder, flatValuesMap, fieldDelimiter, fieldNamePrefix, origSchema, origValue);

    return new Pair<>(flatSchemaBuilder.build(), flatValuesMap);
  }

  private static Struct createPopulatedFlatStruct(Schema flatSchema, Map<String, Object> flatValuesMap) {
    Struct flatStruct = new Struct(flatSchema);
    for (Entry<String, Object> entry : flatValuesMap.entrySet()) {
      flatStruct.put(entry.getKey(), entry.getValue());
    }

    return flatStruct;
  }

  private static void populateFlatSchemaAndValuesMap(SchemaBuilder flatSchemaBuilder, Map<String, Object> flatValuesMap, String fieldDelimiter, String fieldNamePrefix, Schema schema, Struct value) {
    List<Field> schemaFields = schema.fields();
    for (Field field : schemaFields) {
      Schema fieldSchema = field.schema();
      String fieldName = field.name();
      Object fieldValue = value.get(fieldName);

      if (fieldSchema.type() == Schema.Type.STRUCT) {
        String newFieldNamePrefix = fieldNamePrefix + fieldName + fieldDelimiter;
        populateFlatSchemaAndValuesMap(flatSchemaBuilder, flatValuesMap, fieldDelimiter, newFieldNamePrefix, fieldSchema, (Struct) fieldValue);
      } else {
        String fullFieldName = fieldNamePrefix + fieldName;
        flatSchemaBuilder.field(fullFieldName, fieldSchema);
        flatValuesMap.put(fullFieldName, fieldValue);
      }
    }
  }
}
