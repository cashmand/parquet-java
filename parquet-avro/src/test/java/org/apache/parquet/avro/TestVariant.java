/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.avro;

import static org.apache.parquet.avro.AvroTestUtil.*;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.DirectWriterTest;
import org.apache.parquet.Preconditions;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantDuplicateKeyException;
import org.apache.parquet.variant.VariantUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestVariant extends DirectWriterTest {

  private static final LogicalTypeAnnotation STRING = LogicalTypeAnnotation.stringType();

  // Construct a variant. E.g. variant(b -> b.appendBoolean(true))
  private static Variant variant(Consumer<VariantBuilder> appendValue) {
    VariantBuilder builder = new VariantBuilder(false);
    appendValue.accept(builder);
    return builder.result();
  }

  // Returns a Variant value based on buildling with fixed metadata.
  private static Variant variant(byte[] metadata, Consumer<VariantBuilder> appendValue) {
    VariantBuilder builder = new VariantBuilder(false);
    builder.setFixedMetadata(VariantUtil.getMetadataMap(metadata));
    appendValue.accept(builder);
    return new Variant(builder.valueWithoutMetadata(), metadata);
  }

  private static final Variant[] PRIMITIVES =
      new Variant[] {
          variant(b -> b.appendNull()),
          variant(b -> b.appendBoolean(true)),
          variant(b -> b.appendBoolean(false)),
          // TODO: fix types
          variant(b -> b.appendLong(34)),
          variant(b -> b.appendLong(-34)),
          variant(b -> b.appendLong(1234)),
          variant(b -> b.appendLong(-1234)),
          variant(b -> b.appendLong(12345)),
          variant(b -> b.appendLong(-12345)),
          variant(b -> b.appendLong(9876543210L)),
          variant(b -> b.appendLong(-9876543210L)),
          variant(b -> b.appendFloat(10.11F)),
          variant(b -> b.appendFloat(-10.11F)),
          variant(b -> b.appendDouble(14.3D)),
          variant(b -> b.appendDouble(-14.3D)),
          // Dates and timestamps aren't very interesting in Variant tests, since they are passed
          // to and from the API as integers. So just test arbitrary integer values.
          variant(b -> b.appendDate(12345)),
          variant(b -> b.appendDate(-12345)),
          variant(b -> b.appendTimestamp(9876543210L)),
          variant(b -> b.appendTimestamp(-9876543210L)),
          variant(b -> b.appendTimestampNtz(9876543210L)),
          variant(b -> b.appendTimestampNtz(-9876543210L)),
          variant(b -> b.appendTimestampNanos(9876543210L)),
          variant(b -> b.appendTimestampNanos(-9876543210L)),
          variant(b -> b.appendTimestampNanosNtz(9876543210L)),
          variant(b -> b.appendTimestampNanosNtz(-9876543210L)),
          variant(b -> b.appendDecimal(new BigDecimal("123456.7890"))), // decimal4
          variant(b -> b.appendDecimal(new BigDecimal("-123456.7890"))), // decimal4
          variant(b -> b.appendDecimal(new BigDecimal("1234567890.987654321"))), // decimal8
          variant(b -> b.appendDecimal(new BigDecimal("-1234567890.987654321"))), // decimal8
          variant(b -> b.appendDecimal(new BigDecimal("9876543210.123456789"))), // decimal16
          variant(b -> b.appendDecimal(new BigDecimal("-9876543210.123456789"))), // decimal16
          variant(b -> b.appendBinary(new byte[] {0x0a, 0x0b, 0x0c, 0x0d})),
          variant(b -> b.appendString("parquet")),
          variant(b -> b.appendUUID(UUID.fromString("f24f9b64-81fa-49d1-b74e-8c09a6e31c56")))
      };

  private byte[] EMPTY_METADATA = PRIMITIVES[0].getMetadata();
  private Variant NULL_VALUE = PRIMITIVES[0];

  private byte[] TEST_METADATA;

  public TestVariant() throws Exception {
    TEST_METADATA = VariantBuilder.parseJson(
        "{\"a\": 0, \"b\": 0, \"c\": 0, \"d\": 0, \"e\": 0}").getMetadata();
  }

  @Test
  public void testUnshredded() throws Exception {
    // Unshredded Variant should produce exactly the same value and metadata.
    Variant testValue = VariantBuilder.parseJson("{\"a\": 123, \"b\": [\"a\", 2, true, null]}");
    Binary expectedValue = Binary.fromConstantByteArray(testValue.getValue());
    Binary expectedMetadata = Binary.fromConstantByteArray(testValue.getMetadata());
    Path test = writeDirect(
        "message VariantMessage {" + "  required group v (VARIANT(1)) {"
            + "    required binary value;"
            + "    required binary metadata;"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("v", 0);

          rc.startGroup();
          rc.startField("value", 0);
          rc.addBinary(expectedValue);
          rc.endField("value", 0);
          rc.startField("metadata", 1);
          rc.addBinary(expectedMetadata);
          rc.endField("metadata", 1);
          rc.endGroup();

          rc.endField("v", 0);
          rc.endMessage();
        });

    Schema variantSchema = record(
        "v",
        field("metadata", Schema.create(Schema.Type.BYTES)),
        optionalField("value", Schema.create(Schema.Type.BYTES)));
    Schema expectedSchema = record("VariantMessage", field("v", variantSchema));

    GenericRecord expectedRecord = instance(
        expectedSchema,
        "v",
        instance(
            variantSchema,
            "metadata",
            expectedMetadata.toByteBuffer(),
            "value",
            expectedValue.toByteBuffer()));

    // both should behave the same way
    assertReaderContains(new AvroParquetReader(new Configuration(), test), expectedSchema, expectedRecord);
  }

  /**
   * Construct a Variant with a single scalar value, and write the same value to the typed_value column
   * of a shredded file, verifying that the reconstructed value is bit-for-bit identical to the original value.
   * and a lambda to append the same corresponding value to the
   * @param type Type of the shredded field. E.g. int64"
   * @param annotation Logical annotation of the shredded field, or empty string if none. E.g. "UTF8"
   * @param appendValue Lambda to append a value to a VariantBuilder
   * @param addValue Lambda to append the logically equivalent value to a RecordConsumer
   * @throws Exception
   */
  public void runOneScalarTest(
      String type, String annotation, Consumer<VariantBuilder> appendValue, Consumer<RecordConsumer> addValue)
      throws Exception {
    VariantBuilder builder = new VariantBuilder(false);
    appendValue.accept(builder);
    Variant testValue = builder.result();
    Binary expectedValue = Binary.fromConstantByteArray(testValue.getValue());
    Binary expectedMetadata = Binary.fromConstantByteArray(testValue.getMetadata());
    Path test = writeDirect(
        "message VariantMessage {" + "  required group v (VARIANT(1)) {"
            + "    optional binary value;"
            + "    required binary metadata;"
            + "    optional " + type + " typed_value " + annotation + ";"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("v", 0);

          rc.startGroup();
          rc.startField("typed_value", 2);
          addValue.accept(rc);
          rc.endField("typed_value", 2);
          rc.startField("metadata", 1);
          rc.addBinary(expectedMetadata);
          rc.endField("metadata", 1);
          rc.endGroup();

          rc.endField("v", 0);
          rc.endMessage();
        });

    Schema variantSchema = record(
        "v",
        field("metadata", Schema.create(Schema.Type.BYTES)),
        optionalField("value", Schema.create(Schema.Type.BYTES)));
    Schema expectedSchema = record("VariantMessage", field("v", variantSchema));

    GenericRecord expectedRecord = instance(
        expectedSchema,
        "v",
        instance(
            variantSchema,
            "metadata",
            expectedMetadata.toByteBuffer(),
            "value",
            expectedValue.toByteBuffer()));

    // both should behave the same way
    assertReaderContains(new AvroParquetReader(new Configuration(), test), expectedSchema, expectedRecord);
  }

  @Test
  public void testShreddedScalar() throws Exception {
    // TODO: Test VariantNull
    runOneScalarTest("boolean", "", b -> b.appendBoolean(true), rc -> rc.addBoolean(true));
    // Test true and false, since they have different types in Variant.
    runOneScalarTest("boolean", "", b -> b.appendBoolean(false), rc -> rc.addBoolean(false));
    runOneScalarTest("boolean", "", b -> b.appendBoolean(false), rc -> rc.addBoolean(false));
    runOneScalarTest("int32", "(INT_8)", b -> b.appendLong(123), rc -> rc.addInteger(123));
    runOneScalarTest("int32", "(INT_16)", b -> b.appendLong(-12345), rc -> rc.addInteger(-12345));
    runOneScalarTest("int32", "(INT_32)", b -> b.appendLong(1234567890), rc -> rc.addInteger(1234567890));
    runOneScalarTest("int64", "", b -> b.appendLong(1234567890123L), rc -> rc.addLong(1234567890123L));
    runOneScalarTest("double", "", b -> b.appendDouble(1.2e34), rc -> rc.addDouble(1.2e34));
    runOneScalarTest("float", "", b -> b.appendFloat(1.2e34f), rc -> rc.addFloat(1.2e34f));
    runOneScalarTest(
        "int32", "(DECIMAL(9, 2))", b -> b.appendDecimal(new BigDecimal("1.23")), rc -> rc.addInteger(123));
    runOneScalarTest(
        "int64",
        "(DECIMAL(18, 5))",
        b -> b.appendDecimal(new BigDecimal("123456789.12345")),
        rc -> rc.addLong(12345678912345L));
    BigDecimal decimalVal = new BigDecimal("0.12345678901234567890123456789012345678");
    runOneScalarTest(
        "fixed_len_byte_array(16)",
        "(DECIMAL(38, 38))",
        b -> b.appendDecimal(decimalVal),
        rc -> rc.addBinary(
            Binary.fromConstantByteArray(decimalVal.unscaledValue().toByteArray())));
    // Verify that the parquet type's scale is used when shredding, and not the scale implied by the value.
    runOneScalarTest(
        "int32",
        "(DECIMAL(9, 2))",
        b -> b.appendDecimal(new BigDecimal("1.2").setScale(2)),
        rc -> rc.addInteger(120));
    runOneScalarTest(
        "int64",
        "(DECIMAL(18, 5))",
        b -> b.appendDecimal(new BigDecimal("123456789").setScale(5)),
        rc -> rc.addLong(12345678900000L));
    BigDecimal decimalVal2 = new BigDecimal("9.12345678901234567890123456789").setScale(37);
    runOneScalarTest(
        "fixed_len_byte_array(16)",
        "(DECIMAL(38, 37))",
        b -> b.appendDecimal(decimalVal2),
        rc -> rc.addBinary(
            Binary.fromConstantByteArray(decimalVal2.unscaledValue().toByteArray())));
    runOneScalarTest("int64", "(TIMESTAMP(MICROS, true))", b -> b.appendTimestamp(123), rc -> rc.addLong(123));
    runOneScalarTest("int64", "(TIMESTAMP(MICROS, false))", b -> b.appendTimestampNtz(123), rc -> rc.addLong(123));
    runOneScalarTest(
        "binary",
        "",
        b -> b.appendBinary(Binary.fromString("hello").toByteBuffer().array()),
        rc -> rc.addBinary(Binary.fromString("hello")));
    runOneScalarTest(
        "binary", "(UTF8)", b -> b.appendString("hello"), rc -> rc.addBinary(Binary.fromString("hello")));
    runOneScalarTest("int64", "(TIME(MICROS, false))", b -> b.appendTime(123), rc -> rc.addLong(123));
    runOneScalarTest("int64", "(TIMESTAMP(NANOS, true))", b -> b.appendTimestampNanos(123), rc -> rc.addLong(123));
    runOneScalarTest(
        "int64", "(TIMESTAMP(NANOS, false))", b -> b.appendTimestampNanosNtz(123), rc -> rc.addLong(123));
    UUID uuid = UUID.randomUUID();
    byte[] uuidBytes = new byte[16];
    ByteBuffer bb = ByteBuffer.wrap(uuidBytes, 0, 16).order(ByteOrder.BIG_ENDIAN);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    runOneScalarTest(
        "fixed_len_byte_array(16)",
        "(UUID)",
        b -> b.appendUUID(uuid),
        rc -> rc.addBinary(Binary.fromConstantByteArray(uuidBytes)));
  }

  @Test
  public void testArray() throws Exception {
    Variant testValue = VariantBuilder.parseJson("[123, \"hello\", 456]");
    // The string value will be stored in value, not typed_value, so we need to write its binary representation
    // to parquet.
    Variant stringVal = testValue.getElementAtIndex(1);
    byte[] stringValue = stringVal.getValue();

    Binary expectedValue = Binary.fromConstantByteArray(testValue.getValue());
    Binary expectedMetadata = Binary.fromConstantByteArray(testValue.getMetadata());
    // TODO: is there a better way to write this test? All the Start* and End* calls seem excessive.
    Path test = writeDirect(
        "message VariantMessage {" + "  required group v (VARIANT(1)) {"
            + "    required binary metadata;"
            + "    optional binary value;"
            + "    optional group typed_value (LIST) {"
            + "      repeated group list {"
            + "        required group element {"
            + "          optional int64 typed_value;"
            + "          optional binary value;"
            + "        }"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("v", 0);
          rc.startGroup();
          rc.startField("typed_value", 2);
          rc.startGroup();
          rc.startField("list", 0);

          rc.startGroup();
          rc.startField("element", 0);
          rc.startGroup();
          rc.startField("typed_value", 0);
          rc.addLong(123);
          rc.endField("typed_value", 0);
          rc.endGroup();
          rc.endField("element", 0);
          rc.endGroup();

          rc.startGroup();
          rc.startField("element", 0);
          rc.startGroup();
          rc.startField("value", 1);
          rc.addBinary(Binary.fromConstantByteArray(stringValue));
          rc.endField("value", 1);
          rc.endGroup();
          rc.endField("element", 0);
          rc.endGroup();

          rc.startGroup();
          rc.startField("element", 0);
          rc.startGroup();
          rc.startField("typed_value", 0);
          rc.addLong(456);
          rc.endField("typed_value", 0);
          rc.endGroup();
          rc.endField("element", 0);
          rc.endGroup();

          rc.endField("list", 0);
          rc.endGroup();
          rc.endField("typed_value", 2);

          rc.startField("metadata", 0);
          rc.addBinary(expectedMetadata);
          rc.endField("metadata", 0);

          rc.endGroup();
          rc.endField("v", 0);
          rc.endMessage();
        });

    Schema variantSchema = record(
        "v",
        field("metadata", Schema.create(Schema.Type.BYTES)),
        optionalField("value", Schema.create(Schema.Type.BYTES)));
    Schema expectedSchema = record("VariantMessage", field("v", variantSchema));

    GenericRecord expectedRecord = instance(
        expectedSchema,
        "v",
        instance(
            variantSchema,
            "metadata",
            expectedMetadata.toByteBuffer(),
            "value",
            expectedValue.toByteBuffer()));

    // both should behave the same way
    assertReaderContains(new AvroParquetReader(new Configuration(), test), expectedSchema, expectedRecord);
  }

  @Test
  public void testObject() throws Exception {
    Variant testValue = VariantBuilder.parseJson("{\"a\": 123, \"b\": \"string_val\", \"c\": 456}");
    // Column c will be omitted from the schema and stored in the value column as the object {c: 456}.
    // It's a bit tricky to construct this, since we need to ensure that it ends up with the same ID in metadata.
    // The value below should do the trick, but it makes assumptions about parseJson behavior that is not guaranteed,
    // so is a bit fragile.
    Variant cValue = VariantBuilder.parseJson("{\"x\": 1, \"dummy\": {\"c\": 456}}").getFieldByKey("dummy");

    Binary expectedValue = Binary.fromConstantByteArray(testValue.getValue());
    Binary expectedMetadata = Binary.fromConstantByteArray(testValue.getMetadata());
    Path test = writeDirect(
        "message VariantMessage {" + "  required group v (VARIANT(1)) {"
            + "    required binary metadata;"
            + "    optional binary value;"
            + "    optional group typed_value {"
            + "      required group a {"
            + "        optional int64 typed_value;"
            + "        optional binary value;"
            + "      }"
            + "      required group b {"
            + "        optional binary typed_value (UTF8);"
            + "        optional binary value;"
            + "      }"
            + "      required group missing {"
            + "        optional int64 typed_value;"
            + "        optional binary value;"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("v", 0);
          rc.startGroup();
          rc.startField("typed_value", 2);
          rc.startGroup();

          rc.startField("a", 0);
          rc.startGroup();
          rc.startField("typed_value", 0);
          rc.addLong(123);
          rc.endField("typed_value", 0);
          rc.endGroup();
          rc.endField("a", 0);

          rc.startField("b", 1);
          rc.startGroup();
          rc.startField("typed_value", 0);
          rc.addBinary(Binary.fromString("string_val"));
          rc.endField("typed_value", 0);
          rc.endGroup();
          rc.endField("b", 1);

          // Spec requires missing fields to be non-null. They are identified as missing by
          // not having a non-null value or typed_value.
          rc.startField("missing", 2);
          rc.startGroup();
          rc.endGroup();
          rc.endField("missing", 2);

          rc.endGroup();
          rc.endField("typed_value", 2);

          rc.startField("value", 1);
          rc.addBinary(Binary.fromConstantByteArray(cValue.getValue()));
          rc.endField("value", 1);

          rc.startField("metadata", 0);
          rc.addBinary(expectedMetadata);
          rc.endField("metadata", 0);

          rc.endGroup();
          rc.endField("v", 0);
          rc.endMessage();
        });

    Schema variantSchema = record(
        "v",
        field("metadata", Schema.create(Schema.Type.BYTES)),
        optionalField("value", Schema.create(Schema.Type.BYTES)));
    Schema expectedSchema = record("VariantMessage", field("v", variantSchema));

    GenericRecord expectedRecord = instance(
        expectedSchema,
        "v",
        instance(
            variantSchema,
            "metadata",
            expectedMetadata.toByteBuffer(),
            "value",
            expectedValue.toByteBuffer()));

    // both should behave the same way
    assertReaderContains(new AvroParquetReader(new Configuration(), test), expectedSchema, expectedRecord);
  }

  public <T extends IndexedRecord> void assertReaderContains(
        AvroParquetReader<T> reader, Schema expectedSchema, T... expectedRecords) throws IOException {
      for (T expectedRecord : expectedRecords) {
        T actualRecord = reader.read();
        Assert.assertEquals("Should match expected schema", expectedSchema, actualRecord.getSchema());
        Assert.assertEquals("Should match the expected record", expectedRecord, actualRecord);
      }
      Assert.assertNull(
          "Should only contain " + expectedRecords.length + " record" + (expectedRecords.length == 1 ? "" : "s"),
          reader.read());
  }

  // We need to store two copies of the schema: one without the Variant type annotation that is used to construct the
  // Avro schema for writing, and one with type annotation that is used in the actual written parquet schema, and when
  // reading.
  private static class TestSchema {
    MessageType parquetSchema;
    MessageType unannotatedParquetSchema;
    GroupType variantType;
    GroupType unannotatedVariantType;

    TestSchema(Type shreddedType) {
      variantType = variant("var", 2, shreddedType);
      unannotatedVariantType = unannotatedVariant("var", 2, shreddedType);
      parquetSchema = parquetSchema(variantType);
      unannotatedParquetSchema = parquetSchema(unannotatedVariantType);
    }

    TestSchema() {
      variantType = variant("var", 2);
      unannotatedVariantType = unannotatedVariant("var", 2);
      parquetSchema = parquetSchema(variantType);
      unannotatedParquetSchema = parquetSchema(unannotatedVariantType);
    }

    TestSchema(GroupType variantType, GroupType unannotatedVariantType, MessageType parquetSchema, MessageType unannotatedParquetSchema) {
      this.variantType = variantType;
      this.unannotatedVariantType = unannotatedVariantType;
      this.parquetSchema = parquetSchema;
      this.unannotatedParquetSchema = unannotatedParquetSchema;
    }
  }

  // The following tests are based on Iceberg's TestVariantReaders suite.
  @Test
  public void testUnshreddedVariants() throws Exception {
    for (Variant v : PRIMITIVES) {
      TestSchema schema = new TestSchema();

      GenericRecord variant = recordFromMap(schema.unannotatedVariantType,
          ImmutableMap.of("metadata", serialize(v.getMetadata()), "value", serialize(v.getValue())));
      GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));
      GenericRecord actual = writeAndRead(schema, record);
      Assert.assertEquals(actual.get("id"), 1);

      GenericRecord actualVariant = (GenericRecord) actual.get("var");
      assertEquivalent(v, actualVariant);
    }
  }

  @Test
  public void testUnshreddedVariantsWithShreddingSchema() throws Exception {
    for (Variant v : PRIMITIVES) {
      // Parquet schema has a shredded field, but it is unused by the data.
      TestSchema schema = new TestSchema(shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));

      GenericRecord variant = recordFromMap(schema.unannotatedVariantType,
          ImmutableMap.of("metadata", serialize(v.getMetadata()), "value", serialize(v.getValue())));
      GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));
      GenericRecord actual = writeAndRead(schema, record);
      Assert.assertEquals(actual.get("id"), 1);

      GenericRecord actualVariant = (GenericRecord) actual.get("var");
      assertEquivalent(v, actualVariant);
    }
  }

  @Test
  public void testShreddedVariantPrimitives() throws IOException {
    for (Variant v : PRIMITIVES) {
      if (v.getType() == VariantUtil.Type.NULL) {
        // NULL isn't a valid type for shredding.
        continue;
      }
      TestSchema schema = new TestSchema(shreddedType(v));

      GenericRecord variant =
          recordFromMap(
              schema.unannotatedVariantType,
              ImmutableMap.of(
                  "metadata",
                  v.getMetadata(),
                  "typed_value",
                  // TODO: See Ryan's comment: have the test case code produce both the value and variant equivalent.
                  toAvroValue(v)));
      GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

      GenericRecord actual = writeAndRead(schema, record);
      Assert.assertEquals(actual.get("id"), 1);

      GenericRecord actualVariant = (GenericRecord) actual.get("var");
      assertEquivalent(v, actualVariant);
    }
  }

  @Test
  public void testNullValueAndNullTypedValue() throws IOException {
    TestSchema schema = new TestSchema(shreddedPrimitive(PrimitiveTypeName.INT32));

    GenericRecord variant =
        recordFromMap(schema.unannotatedVariantType, ImmutableMap.of("metadata", EMPTY_METADATA));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);
    Assert.assertEquals(actual.get("id"), 1);

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(NULL_VALUE, actualVariant);
  }

  @Test
  public void testMissingValueColumn() throws IOException {
    GroupType variantType =
        Types.buildGroup(Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.variantType((byte) 1))
            .id(2)
            .required(PrimitiveTypeName.BINARY)
            .named("metadata")
            .addField(shreddedPrimitive(PrimitiveTypeName.INT32))
            .named("var");

    GroupType unannotatedVariantType = Types.buildGroup(Type.Repetition.REQUIRED)
            .id(2)
            .required(PrimitiveTypeName.BINARY)
            .named("metadata")
            .addField(shreddedPrimitive(PrimitiveTypeName.INT32))
            .named("var");
    MessageType parquetSchema = parquetSchema(variantType);
    MessageType unannotatedParquetSchema = parquetSchema(unannotatedVariantType);

    TestSchema schema = new TestSchema(variantType, unannotatedVariantType, parquetSchema, unannotatedParquetSchema);

    GenericRecord variant =
        recordFromMap(unannotatedVariantType, ImmutableMap.of("metadata", EMPTY_METADATA, "typed_value", 34));
    GenericRecord record = recordFromMap(unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);
    Assert.assertEquals(actual.get("id"), 1);

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(variant(b -> b.appendLong(34)), actualVariant);
  }

  @Test
  public void testValueAndTypedValueConflict() throws IOException {
    TestSchema schema = new TestSchema(shreddedPrimitive(PrimitiveTypeName.INT32));

    GenericRecord variant =
        recordFromMap(
            schema.unannotatedVariantType,
            ImmutableMap.of(
                "metadata",
                EMPTY_METADATA,
                "value",
                variant(b -> b.appendString("str")).getValue(),
                "typed_value",
                34));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    assertThrows(() -> writeAndRead(schema, record), IllegalArgumentException.class,
        "Invalid variant, conflicting value and typed_value");
  }

  @Test
  public void testUnsignedInteger() {
    TestSchema schema = new TestSchema(shreddedPrimitive(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, false)));

    GenericRecord variant =
        recordFromMap(schema.unannotatedVariantType, ImmutableMap.of("metadata", EMPTY_METADATA));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    assertThrows(() -> writeAndRead(schema, record),
        UnsupportedOperationException.class, "Unsupported shredded value type: INTEGER(32,false)");
  }

  @Test
  public void testFixedLengthByteArray() {
    Type shreddedType = Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(4).named("typed_value");
    TestSchema schema = new TestSchema(shreddedType);

    GenericRecord variant =
        recordFromMap(schema.unannotatedVariantType, ImmutableMap.of("metadata", EMPTY_METADATA));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    assertThrows(() -> writeAndRead(schema, record),
        UnsupportedOperationException.class,
        "Unsupported shredded value type: optional fixed_len_byte_array(4) typed_value");
  }

  @Test
  public void testShreddedObject() throws IOException {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    TestSchema schema = new TestSchema(objectFields);

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of("value", serialize(NULL_VALUE.getValue())));
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of("typed_value", ""));
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant =
        recordFromMap(schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);
    Assert.assertEquals(actual.get("id"), 1);

    Variant expectedValue = VariantBuilder.parseJson(
        "{\"a\": null, \"b\": \"\"}");

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue.getValue(), actualVariant);
  }

  @Test
  public void testShreddedObjectMissingValueColumn() throws IOException {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    GroupType variantType = Types.buildGroup(Type.Repetition.REQUIRED)
        .id(2)
        .as(LogicalTypeAnnotation.variantType((byte) 1))
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .addField(objectFields)
        .named("var");

    GroupType unannotatedVariantType = Types.buildGroup(Type.Repetition.REQUIRED)
        .id(2)
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .addField(objectFields)
        .named("var");

    MessageType parquetSchema = parquetSchema(variantType);
    MessageType unannotatedParquetSchema = parquetSchema(unannotatedVariantType);
    TestSchema schema = new TestSchema(variantType, unannotatedVariantType, parquetSchema, unannotatedParquetSchema);

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of("value",
      variant(b -> b.appendLong(1234)).getValue()));
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of("typed_value", "iceberg"));
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant =
        recordFromMap(schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);
    Assert.assertEquals(actual.get("id"), 1);

    Variant expectedValue = VariantBuilder.parseJson(
        "{\"a\": 1234, \"b\": \"iceberg\"}");

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue.getValue(), actualVariant);
  }

  @Test
  public void testShreddedObjectMissingField() throws IOException {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    TestSchema schema = new TestSchema(objectFields);

    GenericRecord recordA = recordFromMap(fieldA,
        ImmutableMap.of("value", variant(b -> b.appendBoolean(false)).getValue()));
    // value and typed_value are null, but a struct for b is required
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of());
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant =
        recordFromMap(schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);

    Assert.assertEquals(actual.get("id"), 1);

    // TODO: I want to make sure that the metadata IDs are the same, so I need to pre-populate the metadata.
    Variant expectedValue = VariantBuilder.parseJson(
        "{\"a\": false}");

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue.getValue(), actualVariant);
  }

  @Test
  public void testEmptyShreddedObject() throws IOException {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    TestSchema schema = new TestSchema(objectFields);

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of()); // missing
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of()); // missing
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant =
        recordFromMap(schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);

    Assert.assertEquals(actual.get("id"), 1);

    Variant expectedValue = VariantBuilder.parseJson("{}");

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue.getValue(), actualVariant);
  }

  @Test
  public void testShreddedObjectMissingFieldValueColumn() throws IOException {
    // field groups do not have value
    GroupType fieldA =
        Types.buildGroup(Type.Repetition.REQUIRED)
            .addField(shreddedPrimitive(PrimitiveTypeName.INT32))
            .named("a");
    GroupType fieldB =
        Types.buildGroup(Type.Repetition.REQUIRED)
            .addField(shreddedPrimitive(PrimitiveTypeName.BINARY, STRING))
            .named("b");
    GroupType objectFields =
        Types.buildGroup(Type.Repetition.OPTIONAL).addFields(fieldA, fieldB).named("typed_value");
    TestSchema schema = new TestSchema(objectFields);

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of()); // typed_value=null
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of("typed_value", "iceberg"));
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant =
        recordFromMap(schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);

    Assert.assertEquals(actual.get("id"), 1);

    Variant expectedValue = variant(TEST_METADATA, b -> {
            int startWritePos = b.getWritePos();
            ArrayList<VariantBuilder.FieldEntry> entries = new ArrayList<>();
            entries.add(new VariantBuilder.FieldEntry("b", 1, 0));
            b.appendString("iceberg");
            b.finishWritingObject(startWritePos, entries);
      });

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue.getValue(), actualVariant);
  }


  @Test
  public void testShreddedObjectMissingTypedValue() throws IOException {
    // field groups do not have typed_value
    GroupType fieldA =
        Types.buildGroup(Type.Repetition.REQUIRED)
            .optional(PrimitiveTypeName.BINARY)
            .named("value")
            .named("a");
    GroupType fieldB =
        Types.buildGroup(Type.Repetition.REQUIRED)
            .optional(PrimitiveTypeName.BINARY)
            .named("value")
            .named("b");
    GroupType objectFields =
        Types.buildGroup(Type.Repetition.OPTIONAL).addFields(fieldA, fieldB).named("typed_value");
    TestSchema schema = new TestSchema(objectFields);

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of()); // value=null
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of("value", variant(b -> b.appendString("iceberg")).getValue()));
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant =
        recordFromMap(schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);

    Assert.assertEquals(actual.get("id"), 1);

    Variant expectedValue = variant(TEST_METADATA, b -> {
      int startWritePos = b.getWritePos();
      ArrayList<VariantBuilder.FieldEntry> entries = new ArrayList<>();
      entries.add(new VariantBuilder.FieldEntry("b", 1, 0));
      b.appendString("iceberg");
      b.finishWritingObject(startWritePos, entries);
    });

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue.getValue(), actualVariant);
  }

  @Test
  public void testShreddedObjectWithinShreddedObject() throws IOException {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType innerFields = objectFields(fieldA, fieldB);
    GroupType fieldC = shreddedField("c", innerFields);
    GroupType fieldD = shreddedField("d", shreddedPrimitive(PrimitiveTypeName.DOUBLE));
    GroupType outerFields = objectFields(fieldC, fieldD);
    TestSchema schema = new TestSchema(outerFields);

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of("typed_value", 34));
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of("typed_value", "iceberg"));
    GenericRecord inner = recordFromMap(innerFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord recordC = recordFromMap(fieldC, ImmutableMap.of("typed_value", inner));
    GenericRecord recordD = recordFromMap(fieldD, ImmutableMap.of("typed_value", -0.0D));
    GenericRecord outer = recordFromMap(outerFields, ImmutableMap.of("c", recordC, "d", recordD));
    GenericRecord variant =
        recordFromMap(schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", outer));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);

    Assert.assertEquals(actual.get("id"), 1);

    Variant expectedValue = variant(TEST_METADATA, b -> {
      int startWritePos = b.getWritePos();
      ArrayList<VariantBuilder.FieldEntry> outerEntries = new ArrayList<>();

      outerEntries.add(new VariantBuilder.FieldEntry("c", 2, b.getWritePos() - startWritePos));
      ArrayList<VariantBuilder.FieldEntry> innerEntries = new ArrayList<>();
      innerEntries.add(new VariantBuilder.FieldEntry("a", 0, b.getWritePos() - startWritePos));
      b.appendLong(34);
      innerEntries.add(new VariantBuilder.FieldEntry("b", 1, b.getWritePos() - startWritePos));
      b.appendString("iceberg");
      b.finishWritingObject(startWritePos, innerEntries);

      outerEntries.add(new VariantBuilder.FieldEntry("d", 3, b.getWritePos() - startWritePos));
      b.appendDouble(-0.0D);
      b.finishWritingObject(startWritePos, outerEntries);
    });

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue.getValue(), actualVariant);
  }

  @Test
  public void testShreddedObjectWithOptionalFieldStructs() throws IOException {
    // fields use an incorrect OPTIONAL struct of value and typed_value to test definition levels
    GroupType fieldA =
        Types.buildGroup(Type.Repetition.OPTIONAL)
            .optional(PrimitiveTypeName.BINARY)
            .named("value")
            .addField(shreddedPrimitive(PrimitiveTypeName.INT32))
            .named("a");
    GroupType fieldB =
        Types.buildGroup(Type.Repetition.OPTIONAL)
            .optional(PrimitiveTypeName.BINARY)
            .named("value")
            .addField(shreddedPrimitive(PrimitiveTypeName.BINARY, STRING))
            .named("b");
    GroupType fieldC =
        Types.buildGroup(Type.Repetition.OPTIONAL)
            .optional(PrimitiveTypeName.BINARY)
            .named("value")
            .addField(shreddedPrimitive(PrimitiveTypeName.DOUBLE))
            .named("c");
    GroupType fieldD =
        Types.buildGroup(Type.Repetition.OPTIONAL)
            .optional(PrimitiveTypeName.BINARY)
            .named("value")
            .addField(shreddedPrimitive(PrimitiveTypeName.BOOLEAN))
            .named("d");
    GroupType objectFields =
        Types.buildGroup(Type.Repetition.OPTIONAL)
            .addFields(fieldA, fieldB, fieldC, fieldD)
            .named("typed_value");
    TestSchema schema = new TestSchema(objectFields);

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of("value", variant(b -> b.appendLong(34)).getValue()));
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of("typed_value", "iceberg"));
    GenericRecord recordC = recordFromMap(fieldC, ImmutableMap.of()); // c.value and c.typed_value are missing
    GenericRecord fields =
        recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB, "c", recordC)); // d is missing
    GenericRecord variant =
        recordFromMap(schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);

    Assert.assertEquals(actual.get("id"), 1);

    Variant expectedValue = variant(TEST_METADATA, b -> {
      int startWritePos = b.getWritePos();
      ArrayList<VariantBuilder.FieldEntry> entries = new ArrayList<>();
      entries.add(new VariantBuilder.FieldEntry("a", 0, b.getWritePos() - startWritePos));
      b.appendLong(34);
      entries.add(new VariantBuilder.FieldEntry("b", 1, b.getWritePos() - startWritePos));
      b.appendString("iceberg");
      b.finishWritingObject(startWritePos, entries);
    });

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue.getValue(), actualVariant);
  }

  @Test
  public void testPartiallyShreddedObject() throws IOException {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    TestSchema schema = new TestSchema(objectFields);

    byte[] baseObject = variant(TEST_METADATA, b -> {
      int startWritePos = b.getWritePos();
      ArrayList<VariantBuilder.FieldEntry> entries = new ArrayList<>();
      entries.add(new VariantBuilder.FieldEntry("d", 3, b.getWritePos() - startWritePos));
      b.appendDate(12345);
      b.finishWritingObject(startWritePos, entries);
    }).getValue();

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of("value", NULL_VALUE.getValue()));
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of("typed_value", "iceberg"));
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant =
        recordFromMap(
            schema.unannotatedVariantType,
            ImmutableMap.of(
                "metadata",
                TEST_METADATA,
                "value",
                serialize(baseObject),
                "typed_value",
                fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);

    Assert.assertEquals(actual.get("id"), 1);

    Variant expectedValue = variant(TEST_METADATA, b -> {
      int startWritePos = b.getWritePos();
      ArrayList<VariantBuilder.FieldEntry> entries = new ArrayList<>();
      entries.add(new VariantBuilder.FieldEntry("a", 0, b.getWritePos() - startWritePos));
      b.appendNull();
      entries.add(new VariantBuilder.FieldEntry("b", 1, b.getWritePos() - startWritePos));
      b.appendString("iceberg");
      entries.add(new VariantBuilder.FieldEntry("d", 3, b.getWritePos() - startWritePos));
      b.appendDate(12345);
      b.finishWritingObject(startWritePos, entries);
    });

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue.getValue(), actualVariant);
  }

  @Test
  public void testPartiallyShreddedObjectFieldConflict() throws IOException {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    TestSchema schema = new TestSchema(objectFields);

    byte[] baseObject = variant(TEST_METADATA, b -> {
      int startWritePos = b.getWritePos();
      ArrayList<VariantBuilder.FieldEntry> entries = new ArrayList<>();
      entries.add(new VariantBuilder.FieldEntry("b", 1, b.getWritePos() - startWritePos));
      b.appendDate(12345);
      b.finishWritingObject(startWritePos, entries);
    }).getValue();

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of("value", NULL_VALUE.getValue()));
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of("typed_value", "iceberg"));
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant =
        recordFromMap(
            schema.unannotatedVariantType,
            ImmutableMap.of(
                "metadata",
                TEST_METADATA,
                "value",
                baseObject,
                "typed_value",
                fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    // Note: Iceberg does not fail in this case, and uses the value from `typed_value`.
    assertThrows(() -> writeAndRead(schema, record),
        VariantDuplicateKeyException.class,
        "Failed to build Variant because of duplicate object key: b");
  }

  @Test
  public void testPartiallyShreddedObjectMissingFieldConflict() throws IOException {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    TestSchema schema = new TestSchema(objectFields);

    byte[] baseObject = variant(TEST_METADATA, b -> {
      int startWritePos = b.getWritePos();
      ArrayList<VariantBuilder.FieldEntry> entries = new ArrayList<>();
      entries.add(new VariantBuilder.FieldEntry("b", 1, b.getWritePos() - startWritePos));
      b.appendDate(12345);
      b.finishWritingObject(startWritePos, entries);
    }).getValue();

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of("value", NULL_VALUE.getValue()));
    // value and typed_value are null, but a struct for b is required
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of());
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant =
        recordFromMap(
            schema.unannotatedVariantType,
            ImmutableMap.of(
                "metadata",
                TEST_METADATA,
                "value",
                baseObject,
                "typed_value",
                fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);

    Assert.assertEquals(actual.get("id"), 1);

    Variant expectedValue = variant(TEST_METADATA, b -> {
      int startWritePos = b.getWritePos();
      ArrayList<VariantBuilder.FieldEntry> entries = new ArrayList<>();
      entries.add(new VariantBuilder.FieldEntry("a", 0, b.getWritePos() - startWritePos));
      b.appendNull();
      // TODO: This documents the current behaviour, but isn't necessarily what we want: it's probably better to
      // either fail with an error here, or use the value from typed_value (i.e. treat b as missing). Iceberg does
      // the latter. I think doing either one in Parquet would require adding a map lookup for every key we add
      // from the residual value, and skipping or throwing if it's already a field in typed_value.
      entries.add(new VariantBuilder.FieldEntry("b", 1, b.getWritePos() - startWritePos));
      b.appendDate(12345);
      b.finishWritingObject(startWritePos, entries);
    });

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue.getValue(), actualVariant);
  }

  @Test
  public void testNonObjectWithNullShreddedFields() throws IOException {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    TestSchema schema = new TestSchema(objectFields);

    GenericRecord variant =
        recordFromMap(
            schema.unannotatedVariantType,
            ImmutableMap.of("metadata", TEST_METADATA, "value", variant(b -> b.appendLong(34)).getValue()));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);
    Assert.assertEquals(actual.get("id"), 1);

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, variant(b -> b.appendLong(34)).getValue(), actualVariant);
  }

  @Test
  public void testNonObjectWithNonNullShreddedFields() {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    TestSchema schema = new TestSchema(objectFields);

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of("value", NULL_VALUE.getValue()));
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of("value", variant(b -> b.appendLong(9876543210L)).getValue()));
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant =
        recordFromMap(
            schema.unannotatedVariantType,
            ImmutableMap.of(
                "metadata",
                TEST_METADATA,
                "value",
                variant(b -> b.appendLong(34)).getValue(),
                "typed_value",
                fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    assertThrows(() -> writeAndRead(schema, record),
        IllegalArgumentException.class,
        "Invalid variant, conflicting value and typed_value");
  }

  /**
   * This is a custom Parquet writer builder that injects a specific Parquet schema and then uses
   * the Avro object model. This ensures that the Parquet file's schema is exactly what was passed.
   */
  private static class TestWriterBuilder
      extends ParquetWriter.Builder<GenericRecord, TestWriterBuilder> {
    private TestSchema schema = null;

    protected TestWriterBuilder(Path path) {
      super(path);
    }

    TestWriterBuilder withFileType(TestSchema schema) {
      this.schema = schema;
      return self();
    }

    @Override
    protected TestWriterBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<GenericRecord> getWriteSupport(Configuration conf) {
      return new AvroWriteSupport<>(schema.parquetSchema, avroSchema(schema.unannotatedParquetSchema), GenericData.get());
    }
  }

  GenericRecord writeAndRead(TestSchema testSchema, GenericRecord record)
      throws IOException {
    // Copied from TestSpecificReadWrite.java. Why does it do these weird things?
    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path path = new Path(tmp.getPath());

    try (ParquetWriter<GenericRecord> writer =
             new TestWriterBuilder(path).withFileType(testSchema).withConf(CONF).build()) {
      writer.write(record);
    }

    Configuration conf = new Configuration();
    // We need to set an explicit read schema, because Avro wrote the shredding schema as the Avro schema in the
    // write, and it will use that by default. If we write using a proper shredding writer, the Avro schema
    // should just contain a <metadata, value> record, and we won't need this.
    AvroReadSupport.setAvroReadSchema(conf, avroSchema(testSchema.parquetSchema));
    AvroParquetReader<GenericRecord> reader = new AvroParquetReader(conf, path);
    GenericRecord result = reader.read();
    assert(reader.read() == null);
    return result;
  }

  private static MessageType parquetSchema(Type variantType) {
    return Types.buildMessage()
        .required(PrimitiveTypeName.INT32)
        .id(1)
        .named("id")
        .addField(variantType)
        .named("table");
  }

  private static Type shreddedType(Variant value) {
    switch (value.getType()) {
      case BOOLEAN:
        return shreddedPrimitive(PrimitiveTypeName.BOOLEAN);
      case BYTE:
        return shreddedPrimitive(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8));
      case SHORT:
        return shreddedPrimitive(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(16));
      case INT:
        return shreddedPrimitive(PrimitiveTypeName.INT32);
      case LONG:
        return shreddedPrimitive(PrimitiveTypeName.INT64);
      case FLOAT:
        return shreddedPrimitive(PrimitiveTypeName.FLOAT);
      case DOUBLE:
        return shreddedPrimitive(PrimitiveTypeName.DOUBLE);
      case DECIMAL:
        int precision = value.getDecimal().precision();
        int scale = value.getDecimal().scale ();
        if (precision <= 9) {
          return shreddedPrimitive(
              PrimitiveTypeName.INT32, LogicalTypeAnnotation.decimalType(scale, 9));
        } else if (precision <= 18) {
          return shreddedPrimitive(
              PrimitiveTypeName.INT64, LogicalTypeAnnotation.decimalType(scale, 18));
        } else {
          return shreddedPrimitive(
              PrimitiveTypeName.BINARY, LogicalTypeAnnotation.decimalType(scale, 38));
        }
      case DATE:
        return shreddedPrimitive(PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType());
      case TIMESTAMP:
        return shreddedPrimitive(
            PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS));
      case TIMESTAMP_NTZ:
        return shreddedPrimitive(
            PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(false, TimeUnit.MICROS));
      case BINARY:
        return shreddedPrimitive(PrimitiveTypeName.BINARY);
      case STRING:
        return shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
      case TIME:
        return shreddedPrimitive(
            PrimitiveTypeName.INT64, LogicalTypeAnnotation.timeType(false, TimeUnit.MICROS));
      case TIMESTAMP_NANOS:
        return shreddedPrimitive(
            PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(true, TimeUnit.NANOS));
      case TIMESTAMP_NANOS_NTZ:
        return shreddedPrimitive(
            PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(false, TimeUnit.NANOS));
      case UUID:
        return shreddedPrimitive(
            PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, LogicalTypeAnnotation.uuidType());
    }

    throw new UnsupportedOperationException("Unsupported shredding type: " + value.getType());
  }

  private static Object toAvroValue(Variant v) {
    switch (v.getType()) {
      case BOOLEAN:
        return v.getBoolean();
      case BYTE:
        return v.getByte();
      case SHORT:
        return v.getShort();
      case INT:
        return v.getInt();
      case LONG:
        return v.getLong();
      case FLOAT:
        return v.getFloat();
      case DOUBLE:
        return v.getDouble();
      case DECIMAL:
        int precision = v.getDecimal().precision();
        int scale = v.getDecimal().scale ();
        if (precision <= 9) {
          return v.getDecimal().unscaledValue().intValueExact();
        } else if (precision <= 18) {
          return v.getDecimal().unscaledValue().longValueExact();
        } else {
          return v.getDecimal().unscaledValue().toByteArray();
        }
      case DATE:
        // TODO: Replace getInt and getLong with appropriate types.
        return v.getInt();
      case TIMESTAMP:
        return v.getLong();
      case TIMESTAMP_NTZ:
        return v.getLong();
      case BINARY:
        return v.getBinary();
      case STRING:
        return v.getString();
      case TIME:
        return v.getLong();
      case TIMESTAMP_NANOS:
        return v.getLong();
      case TIMESTAMP_NANOS_NTZ:
        return v.getLong();
      case UUID:
        return v.getUUID();
      default:
        throw new UnsupportedOperationException("Unsupported shredding type: " + v.getType());
    }
  }

  private static GroupType variant(String name, int fieldId) {
    return Types.buildGroup(Type.Repetition.REQUIRED)
        .id(fieldId)
        .as(LogicalTypeAnnotation.variantType((byte) 1))
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .required(PrimitiveTypeName.BINARY)
        .named("value")
        .named(name);
  }

  private static GroupType unannotatedVariant(String name, int fieldId) {
    return Types.buildGroup(Type.Repetition.REQUIRED)
        .id(fieldId)
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .required(PrimitiveTypeName.BINARY)
        .named("value")
        .named(name);
  }

  private static GroupType variant(String name, int fieldId, Type shreddedType) {
    checkShreddedType(shreddedType);
    return Types.buildGroup(Type.Repetition.OPTIONAL)
        .id(fieldId)
        .as(LogicalTypeAnnotation.variantType((byte) 1))
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .optional(PrimitiveTypeName.BINARY)
        .named("value")
        .addField(shreddedType)
        .named(name);
  }

  // Shredding schema with no Variant logical annotation. Needed in order to construct the Avro schema.
  private static GroupType unannotatedVariant(String name, int fieldId, Type shreddedType) {
    checkShreddedType(shreddedType);
    return Types.buildGroup(Type.Repetition.OPTIONAL)
        .id(fieldId)
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .optional(PrimitiveTypeName.BINARY)
        .named("value")
        .addField(shreddedType)
        .named(name);
  }

  private static void checkField(GroupType fieldType) {
    Preconditions.checkArgument(
        fieldType.isRepetition(Type.Repetition.REQUIRED),
        "Invalid field type repetition: %s should be REQUIRED",
        fieldType.getRepetition());
  }

  private static GroupType objectFields(GroupType... fields) {
    for (GroupType fieldType : fields) {
      checkField(fieldType);
    }

    return Types.buildGroup(Type.Repetition.OPTIONAL).addFields(fields).named("typed_value");
  }
  private static Type shreddedPrimitive(PrimitiveTypeName primitive) {
    return Types.optional(primitive).named("typed_value");
  }

  private static Type shreddedPrimitive(
      PrimitiveTypeName primitive, LogicalTypeAnnotation annotation) {
    return Types.optional(primitive).as(annotation).named("typed_value");
  }

  // TODO: can probably remove this and all callers of it.
  private static ByteBuffer serialize(byte[] bytes) {
    return ByteBuffer.wrap(bytes);
  }

  /** Creates an Avro record from a map of field name to value. */
  private static GenericRecord recordFromMap(GroupType type, Map<String, Object> fields) {
    GenericRecord record = new GenericData.Record(avroSchema(type));
    for (Map.Entry<String, Object> entry : fields.entrySet()) {
      record.put(entry.getKey(), entry.getValue());
    }
    return record;
  }

  // Required configuration to convert between Avro and Parquet schemas with 3-level list structure
  private static final ParquetConfiguration CONF =
      new PlainParquetConfiguration(
          ImmutableMap.of(
              AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE,
              "false",
              AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS,
              "false"));


  private static GroupType shreddedField(String name, Type shreddedType) {
    checkShreddedType(shreddedType);
    return Types.buildGroup(Type.Repetition.REQUIRED)
        .optional(PrimitiveTypeName.BINARY)
        .named("value")
        .addField(shreddedType)
        .named(name);
  }

  private static GroupType element(Type shreddedType) {
    return shreddedField("element", shreddedType);
  }

  private static GroupType list(GroupType elementType) {
    return Types.optionalList().element(elementType).named("typed_value");
  }

  private static void checkListType(GroupType listType) {
    // Check the list is a 3-level structure
    Preconditions.checkArgument(
        listType.getFieldCount() == 1
            && listType.getFields().get(0).isRepetition(Type.Repetition.REPEATED),
        "Invalid list type: does not contain single repeated field: %s",
        listType);

    GroupType repeated = listType.getFields().get(0).asGroupType();
    Preconditions.checkArgument(
        repeated.getFieldCount() == 1
            && repeated.getFields().get(0).isRepetition(Type.Repetition.REQUIRED),
        "Invalid list type: does not contain single required subfield: %s",
        listType);
  }

  private static org.apache.avro.Schema avroSchema(GroupType schema) {
    if (schema instanceof MessageType) {
      return new AvroSchemaConverter(CONF).convert((MessageType) schema);

    } else {
      MessageType wrapped = Types.buildMessage().addField(schema).named("table");
      org.apache.avro.Schema avro =
          new AvroSchemaConverter(CONF).convert(wrapped).getFields().get(0).schema();
      switch (avro.getType()) {
        case RECORD:
          return avro;
        case UNION:
          return avro.getTypes().get(1);
      }

      throw new IllegalArgumentException("Invalid converted type: " + avro);
    }
  }

  private static void checkShreddedType(Type shreddedType) {
    Preconditions.checkArgument(
        shreddedType.getName().equals("typed_value"),
        "Invalid shredded type name: %s should be typed_value",
        shreddedType.getName());
    Preconditions.checkArgument(
        shreddedType.isRepetition(Type.Repetition.OPTIONAL),
        "Invalid shredded type repetition: %s should be OPTIONAL",
        shreddedType.getRepetition());
  }

  // Check for the given excpetion with message, possibly wrapped by a ParquetDecodingException
  void assertThrows(Callable callable, Class<? extends Exception> exception, String msg) {
    try {
      callable.call();
      Assert.fail("No exception was thrown. Expected: " + exception.getName());
    } catch (Exception actual) {
      try {
        if (actual.getClass().equals(ParquetDecodingException.class)) {
          Assert.assertTrue(actual.getCause().getMessage().contains(msg));
          Assert.assertEquals(actual.getCause().getClass(), exception);
        } else {
          Assert.assertTrue(actual.getMessage().contains(msg));
          Assert.assertEquals(actual.getClass(), exception);
        }
      } catch (AssertionError e) {
        e.addSuppressed(actual);
        throw e;
      }
    }
  }

  // Assert that metadata contains identical bytes to expected, and value is logically equivalent.
  // E.g. object fields may be ordered differently in the binary.
  void assertEquivalent(byte[] expectedMetadata, byte[] expectedValue, GenericRecord actual) {
    Assert.assertEquals(ByteBuffer.wrap(expectedMetadata), (ByteBuffer) actual.get("metadata"));
    // TODO: Implement logical equivalence check.
    Assert.assertEquals(ByteBuffer.wrap(expectedValue), (ByteBuffer) actual.get("value"));
  }

  void assertEquivalent(Variant expected, GenericRecord actual) {
    assertEquivalent(expected.getMetadata(), expected.getValue(), actual);
  }

}
