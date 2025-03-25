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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import com.google.common.collect.ImmutableList;
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
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantBuilder;
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

  @Test
  public void testUnshredded() throws Exception {
    // Unshredded Variant should produce exactly the same value and metadata.
    Variant testValue = VariantBuilder.parseJson("{\"a\": 123, \"b\": [\"a\", 2, true, null]}");
    Binary expectedValue = Binary.fromConstantByteArray(testValue.getValue());
    Binary expectedMetadata = Binary.fromConstantByteArray(testValue.getMetadata());
    Path test = writeDirect(
        "message VariantMessage {" + "  required group v {"
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
        "message VariantMessage {" + "  required group v {"
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
        "message VariantMessage {" + "  required group v {"
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
        "message VariantMessage {" + "  required group v {"
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

  // The following tests are based on Iceberg's TestVariantReaders suite.
  @Test
  public void testUnshreddedVariants() throws Exception {
    for (Variant v : PRIMITIVES) {
      GroupType variantType = variant("var", 2);
      MessageType parquetSchema = parquetSchema(variantType);

      GenericRecord variant = recordFromMap(variantType,
          ImmutableMap.of("metadata", serialize(v.getMetadata()), "value", serialize(v.getValue())));
      GenericRecord record = recordFromMap(parquetSchema, ImmutableMap.of("id", 1, "var", variant));
      GenericRecord actual = writeAndRead(parquetSchema, record);
      Assert.assertEquals("Should match the expected record", record, actual);
    }
  }

  @Test
  public void testUnshreddedVariantsWithShreddingSchema() throws Exception {
    for (Variant v : PRIMITIVES) {
      // Parquet schema has a shredded field, but it is unused by the data.
      GroupType variantType = variant("var", 2, shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
      MessageType parquetSchema = parquetSchema(variantType);

      GenericRecord variant = recordFromMap(variantType,
          ImmutableMap.of("metadata", serialize(v.getMetadata()), "value", serialize(v.getValue())));
      GenericRecord record = recordFromMap(parquetSchema, ImmutableMap.of("id", 1, "var", variant));
      GenericRecord actual = writeAndRead(parquetSchema, record);
      Assert.assertEquals("Should match the expected record", record, actual);
    }
  }

  @Test
  public void testShreddedVariantPrimitives() throws IOException {
    for (Variant v : PRIMITIVES) {
      if (v.getType() == VariantUtil.Type.NULL) {
        // NULL isn't a valid type for shredding.
        continue;
      }
      GroupType variantType = variant("var", 2, shreddedType(v));
      MessageType parquetSchema = parquetSchema(variantType);

      GenericRecord variant =
          recordFromMap(
              variantType,
              ImmutableMap.of(
                  "metadata",
                  v.getMetadata(),
                  "typed_value",
                  toAvroValue(v)));
      GenericRecord record = recordFromMap(parquetSchema, ImmutableMap.of("id", 1, "var", variant));

      GenericRecord actual = writeAndRead(parquetSchema, record);
      Assert.assertEquals(actual.get("id"), 1);

      GenericRecord actualVariant = (GenericRecord) actual.get("var");
      Assert.assertEquals(v.getValue(), actualVariant.get("value"));
      Assert.assertEquals(v.getMetadata(), actualVariant.get("metadata"));
    }
  }

  /**
   * This is a custom Parquet writer builder that injects a specific Parquet schema and then uses
   * the Avro object model. This ensures that the Parquet file's schema is exactly what was passed.
   */
  private static class TestWriterBuilder
      extends ParquetWriter.Builder<GenericRecord, TestWriterBuilder> {
    private MessageType parquetSchema = null;

    protected TestWriterBuilder(Path path) {
      super(path);
    }

    TestWriterBuilder withFileType(MessageType schema) {
      this.parquetSchema = schema;
      return self();
    }

    @Override
    protected TestWriterBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<GenericRecord> getWriteSupport(Configuration conf) {
      return new AvroWriteSupport<>(parquetSchema, avroSchema(parquetSchema), GenericData.get());
    }
  }

  GenericRecord writeAndRead(MessageType parquetSchema, GenericRecord record)
      throws IOException {
    // Copied from TestSpecificReadWrite.java. Why does it do these weird things?
    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path path = new Path(tmp.getPath());

    try (ParquetWriter<GenericRecord> writer =
             new TestWriterBuilder(path).withFileType(parquetSchema).withConf(CONF).build()) {
      writer.write(record);
    }

    AvroParquetReader<GenericRecord> reader = new AvroParquetReader(new Configuration(), path);
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
        .as(LogicalTypeAnnotation.variantType())
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
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .optional(PrimitiveTypeName.BINARY)
        .named("value")
        .addField(shreddedType)
        .named(name);
  }

  private static Type shreddedPrimitive(PrimitiveTypeName primitive) {
    return Types.optional(primitive).named("typed_value");
  }

  private static Type shreddedPrimitive(
      PrimitiveTypeName primitive, LogicalTypeAnnotation annotation) {
    return Types.optional(primitive).as(annotation).named("typed_value");
  }

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

}
