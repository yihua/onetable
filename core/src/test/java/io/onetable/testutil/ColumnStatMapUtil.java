/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package io.onetable.testutil;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.stat.Range;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ColumnStatMapUtil {
  private static final OneField LONG_FIELD =
      OneField.builder()
          .name("long_field")
          .schema(OneSchema.builder().name("long").dataType(OneType.LONG).build())
          .build();

  private static final OneField STRING_FIELD =
      OneField.builder()
          .name("string_field")
          .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
          .build();
  private static final OneField NULL_STRING_FIELD =
      OneField.builder()
          .name("null_string_field")
          .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
          .build();
  private static final OneField TIMESTAMP_FIELD =
      OneField.builder()
          .name("timestamp_field")
          .schema(
              OneSchema.builder()
                  .name("long")
                  .dataType(OneType.TIMESTAMP)
                  .metadata(
                      Collections.singletonMap(
                          OneSchema.MetadataKey.TIMESTAMP_PRECISION,
                          OneSchema.MetadataValue.MILLIS))
                  .build())
          .build();
  private static final OneField TIMESTAMP_MICROS_FIELD =
      OneField.builder()
          .name("timestamp_micros_field")
          .schema(
              OneSchema.builder()
                  .name("long")
                  .dataType(OneType.TIMESTAMP)
                  .metadata(
                      Collections.singletonMap(
                          OneSchema.MetadataKey.TIMESTAMP_PRECISION,
                          OneSchema.MetadataValue.MICROS))
                  .build())
          .build();
  private static final OneField LOCAL_TIMESTAMP_FIELD =
      OneField.builder()
          .name("local_timestamp_field")
          .schema(
              OneSchema.builder()
                  .name("long")
                  .dataType(OneType.TIMESTAMP_NTZ)
                  .metadata(
                      Collections.singletonMap(
                          OneSchema.MetadataKey.TIMESTAMP_PRECISION,
                          OneSchema.MetadataValue.MILLIS))
                  .build())
          .build();
  private static final OneField DATE_FIELD =
      OneField.builder()
          .name("date_field")
          .schema(OneSchema.builder().name("int").dataType(OneType.DATE).build())
          .build();

  private static final OneField ARRAY_LONG_FIELD_ELEMENT =
      OneField.builder()
          .name(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME)
          .parentPath("array_long_field")
          .schema(OneSchema.builder().name("long").dataType(OneType.LONG).build())
          .build();
  private static final OneField ARRAY_LONG_FIELD =
      OneField.builder()
          .name("array_long_field")
          .schema(
              OneSchema.builder()
                  .name("array")
                  .dataType(OneType.LIST)
                  .fields(Collections.singletonList(ARRAY_LONG_FIELD_ELEMENT))
                  .build())
          .build();

  private static final OneField MAP_KEY_STRING_FIELD =
      OneField.builder()
          .name(OneField.Constants.MAP_KEY_FIELD_NAME)
          .parentPath("map_string_long_field")
          .schema(OneSchema.builder().name("map_key").dataType(OneType.STRING).build())
          .build();
  private static final OneField MAP_VALUE_LONG_FIELD =
      OneField.builder()
          .name(OneField.Constants.MAP_VALUE_FIELD_NAME)
          .parentPath("map_string_long_field")
          .schema(OneSchema.builder().name("long").dataType(OneType.LONG).build())
          .build();
  private static final OneField MAP_STRING_LONG_FIELD =
      OneField.builder()
          .name("map_string_long_field")
          .schema(
              OneSchema.builder()
                  .name("map")
                  .dataType(OneType.MAP)
                  .fields(Arrays.asList(MAP_KEY_STRING_FIELD, MAP_VALUE_LONG_FIELD))
                  .build())
          .build();

  private static final OneField NESTED_ARRAY_STRING_FIELD_ELEMENT =
      OneField.builder()
          .name(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME)
          .parentPath("nested_struct_field.array_string_field")
          .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
          .build();
  private static final OneField NESTED_ARRAY_STRING_FIELD =
      OneField.builder()
          .name("array_string_field")
          .parentPath("nested_struct_field")
          .schema(
              OneSchema.builder()
                  .name("array")
                  .dataType(OneType.LIST)
                  .fields(Collections.singletonList(NESTED_ARRAY_STRING_FIELD_ELEMENT))
                  .build())
          .build();

  private static final OneField NESTED_LONG_FIELD =
      OneField.builder()
          .name("nested_long_field")
          .parentPath("nested_struct_field")
          .schema(OneSchema.builder().name("long").dataType(OneType.LONG).build())
          .build();

  private static final OneField NESTED_STRUCT_FIELD =
      OneField.builder()
          .name("nested_struct_field")
          .schema(
              OneSchema.builder()
                  .name("nested_struct_field")
                  .dataType(OneType.RECORD)
                  .fields(Arrays.asList(NESTED_ARRAY_STRING_FIELD, NESTED_LONG_FIELD))
                  .build())
          .build();

  private static final OneField DECIMAL_FIELD =
      OneField.builder()
          .name("decimal_field")
          .schema(OneSchema.builder().name("decimal").dataType(OneType.DECIMAL).build())
          .build();

  private static final OneField FLOAT_FIELD =
      OneField.builder()
          .name("float_field")
          .schema(OneSchema.builder().name("float").dataType(OneType.FLOAT).build())
          .build();

  private static final OneField DOUBLE_FIELD =
      OneField.builder()
          .name("double_field")
          .schema(OneSchema.builder().name("double").dataType(OneType.DOUBLE).build())
          .build();

  public static OneSchema getSchema() {
    return OneSchema.builder()
        .name("record")
        .dataType(OneType.RECORD)
        .fields(
            Arrays.asList(
                LONG_FIELD,
                STRING_FIELD,
                NULL_STRING_FIELD,
                TIMESTAMP_FIELD,
                TIMESTAMP_MICROS_FIELD,
                LOCAL_TIMESTAMP_FIELD,
                DATE_FIELD,
                ARRAY_LONG_FIELD,
                MAP_STRING_LONG_FIELD,
                NESTED_STRUCT_FIELD,
                DECIMAL_FIELD,
                FLOAT_FIELD,
                DOUBLE_FIELD))
        .build();
  }

  public static Map<OneField, ColumnStat> getColumnStatMap() {
    ColumnStat longColumnStats =
        ColumnStat.builder()
            .numNulls(4)
            .range(Range.vector(10L, 20L))
            .numValues(50)
            .totalSize(123)
            .build();
    ColumnStat stringColumnStats =
        ColumnStat.builder()
            .numNulls(1)
            .range(Range.vector("a", "c"))
            .numValues(50)
            .totalSize(500)
            .build();
    ColumnStat nullStringColumnStats =
        ColumnStat.builder()
            .numNulls(3)
            .range(Range.vector(null, null))
            .numValues(50)
            .totalSize(0)
            .build();
    ColumnStat timeStampColumnStats =
        ColumnStat.builder()
            .numNulls(105)
            .range(Range.vector(1665263297000L, 1665436097000L))
            .numValues(50)
            .totalSize(999)
            .build();
    ColumnStat timeStampMicrosColumnStats =
        ColumnStat.builder()
            .numNulls(1)
            .range(Range.vector(1665263297000000L, 1665436097000000L))
            .numValues(50)
            .totalSize(400)
            .build();
    ColumnStat localTimeStampColumnStats =
        ColumnStat.builder()
            .numNulls(1)
            .range(Range.vector(1665263297000L, 1665436097000L))
            .numValues(50)
            .totalSize(400)
            .build();
    ColumnStat dateColumnStats =
        ColumnStat.builder()
            .numNulls(250)
            .range(Range.vector(18181, 18547))
            .numValues(50)
            .totalSize(12345)
            .build();
    ColumnStat ignoredColumnStats =
        ColumnStat.builder().numNulls(0).range(Range.scalar("IGNORED")).build();
    ColumnStat arrayLongElementColumnStats =
        ColumnStat.builder()
            .numNulls(2)
            .range(Range.vector(50L, 100L))
            .numValues(50)
            .totalSize(1234)
            .build();
    ColumnStat mapKeyStringColumnStats =
        ColumnStat.builder()
            .numNulls(3)
            .range(Range.vector("key1", "key2"))
            .numValues(50)
            .totalSize(1234)
            .build();
    ColumnStat mapValueLongColumnStats =
        ColumnStat.builder()
            .numNulls(3)
            .range(Range.vector(200L, 300L))
            .numValues(50)
            .totalSize(1234)
            .build();
    ColumnStat nestedArrayStringElementColumnStats =
        ColumnStat.builder()
            .numNulls(7)
            .range(Range.vector("nested1", "nested2"))
            .numValues(50)
            .totalSize(1234)
            .build();
    ColumnStat nestedLongColumnStats =
        ColumnStat.builder()
            .numNulls(4)
            .range(Range.vector(500L, 600L))
            .numValues(50)
            .totalSize(1234)
            .build();
    ColumnStat decimalColumnStats =
        ColumnStat.builder()
            .numNulls(1)
            .range(Range.vector(new BigDecimal("1.0"), new BigDecimal("2.0")))
            .numValues(50)
            .totalSize(123)
            .build();
    ColumnStat floatColumnStats =
        ColumnStat.builder()
            .numNulls(2)
            .range(Range.vector(1.23f, 6.54321f))
            .numValues(50)
            .totalSize(123)
            .build();
    ColumnStat doubleColumnStats =
        ColumnStat.builder()
            .numNulls(3)
            .range(Range.vector(1.23, 6.54321))
            .numValues(50)
            .totalSize(123)
            .build();

    Map<OneField, ColumnStat> columnStatMap = new HashMap<>();
    columnStatMap.put(LONG_FIELD, longColumnStats);
    columnStatMap.put(STRING_FIELD, stringColumnStats);
    columnStatMap.put(NULL_STRING_FIELD, nullStringColumnStats);
    columnStatMap.put(TIMESTAMP_FIELD, timeStampColumnStats);
    columnStatMap.put(TIMESTAMP_MICROS_FIELD, timeStampMicrosColumnStats);
    columnStatMap.put(LOCAL_TIMESTAMP_FIELD, localTimeStampColumnStats);
    columnStatMap.put(DATE_FIELD, dateColumnStats);
    columnStatMap.put(ARRAY_LONG_FIELD, ignoredColumnStats);
    columnStatMap.put(ARRAY_LONG_FIELD_ELEMENT, arrayLongElementColumnStats);
    columnStatMap.put(MAP_STRING_LONG_FIELD, ignoredColumnStats);
    columnStatMap.put(MAP_KEY_STRING_FIELD, mapKeyStringColumnStats);
    columnStatMap.put(MAP_VALUE_LONG_FIELD, mapValueLongColumnStats);
    columnStatMap.put(NESTED_STRUCT_FIELD, ignoredColumnStats);
    columnStatMap.put(NESTED_ARRAY_STRING_FIELD, ignoredColumnStats);
    columnStatMap.put(NESTED_ARRAY_STRING_FIELD_ELEMENT, nestedArrayStringElementColumnStats);
    columnStatMap.put(NESTED_LONG_FIELD, nestedLongColumnStats);
    columnStatMap.put(DECIMAL_FIELD, decimalColumnStats);
    columnStatMap.put(FLOAT_FIELD, floatColumnStats);
    columnStatMap.put(DOUBLE_FIELD, doubleColumnStats);
    return columnStatMap;
  }
}
