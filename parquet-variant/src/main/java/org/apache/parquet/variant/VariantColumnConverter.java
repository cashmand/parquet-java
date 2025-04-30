/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.variant;

import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Stores the Variant builder and metadata used to rebuild a single Variant value from its shredded representation.
 */
class VariantBuilderHolder {
  VariantBuilder builder = null;
  Binary metadata = null;
  // Maps metadata entries to their index in the metadata binary.
  HashMap<String, Integer> metadataMap = null;

  void startNewVariant() {
    builder = new VariantBuilder(false);
  }

  Binary getMetadata() {
    return metadata;
  }

  /**
   * Sets the metadata. May only be called after startNewVariant. We allow the `value` column to
   * be added to the builder before metadata has been set, since it does not depend on metadata, but
   * typed_value (specifically, if typed_value is or contains an object) must be added after setting
   * the metadata.
   */
  void setMetadata(Binary metadata) {
    // If the metadata hasn't changed, we don't need to rebuild the map.
    // When metaata is dictionary encoded, we could consider keeping the map
    // around for every dictionary value, but that could be expensive, and handling adjacent
    // rows with identical metadata should be the most common case.
    if (this.metadata != metadata) {
      metadataMap = VariantUtil.getMetadataMap(metadata.getBytes());
    }
    this.metadata = metadata;
    builder.setFixedMetadata(metadataMap);
  }
}

interface VariantConverter {
  void init(VariantBuilderHolder builderHolder);
}

/**
 * Converter for shredded Variant.
 * The top-level converter is handled by a subclass that also reads metadata.
 * The GroupConverter's children are a value, typed_value, and (optionally) metadata.
 *
 * Values in `typed_value` are appended by the child converter. Values in `value` are stored by the
 * child converter, but only appended when completing this group. Additionally, object fields are
 * appended by the `typed_value` converter, but because residual values are stored in `value`, this
 * converter is responsible for finalizing the object.
 */
class VariantElementConverter extends GroupConverter implements VariantConverter {

  // startWritePos has two uses:
  // 1) If typed_value is an object, we gather fields from value and typed_value and write the final
  // object in end(), so we need to remember the start position.
  // 2) If this is the field of an object, we use startWritePos to tell out parent the field's
  // offset within the encoded parent object.
  private int startWritePos;
  private boolean typedValueIsObject = false;
  private int valueIdx = -1;
  private int typedValueIdx = -1;
  protected VariantBuilderHolder builder;
  protected Converter[] converters;

  // The following are only used if this is an object field.
  private String objectFieldName = null;
  private int objectFieldId = -1;
  private VariantObjectConverter parent = null;

  @Override
  public void init(VariantBuilderHolder builder) {
    this.builder = builder;
    for (Converter converter : converters) {
      if (converter != null) {
        ((VariantConverter) converter).init(builder);
      }
    }
  }

  public VariantElementConverter(GroupType variantSchema, String objectFieldName, VariantObjectConverter parent) {
    this(variantSchema);
    this.objectFieldName = objectFieldName;
    this.parent = parent;
  }

  public VariantElementConverter(GroupType variantSchema) {
    converters = new Converter[3];

    List<Type> fields = variantSchema.getFields();

    for (int i = 0; i < fields.size(); i++) {
      Type field = fields.get(i);
      String fieldName = field.getName();
      if (fieldName.equals("value")) {
        this.valueIdx = i;
        if (!field.isPrimitive() || field.asPrimitiveType().getPrimitiveTypeName() != BINARY) {
          throw new IllegalArgumentException("Value must be a binary value");
        }
      } else if (fieldName.equals("typed_value")) {
        this.typedValueIdx = i;
      }
    }

    if (valueIdx >= 0) {
      converters[valueIdx] = new VariantValueConverter(this);
    }
    if (typedValueIdx >= 0) {
      Converter typedConverter = null;
      Type field = fields.get(typedValueIdx);
      LogicalTypeAnnotation annotation = field.getLogicalTypeAnnotation();
      if (annotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
        typedConverter = new VariantArrayConverter(field.asGroupType());
      } else if (!field.isPrimitive()) {
        typedConverter = new VariantObjectConverter(field.asGroupType());
        typedValueIsObject = true;
      } else {
        PrimitiveType primitive = field.asPrimitiveType();
        PrimitiveType.PrimitiveTypeName primitiveType = primitive.getPrimitiveTypeName();
        if (primitiveType == BOOLEAN) {
          typedConverter = new VariantBooleanConverter();
        } else if (annotation instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
          LogicalTypeAnnotation.IntLogicalTypeAnnotation intAnnotation =
              (LogicalTypeAnnotation.IntLogicalTypeAnnotation) annotation;
          if (!intAnnotation.isSigned()) {
            throw new UnsupportedOperationException("Unsupported shredded value type: " +
              intAnnotation.toString());
          }
          int width = intAnnotation.getBitWidth();
          if (width == 8) {
            typedConverter = new VariantByteConverter();
          } else if (width == 16) {
            typedConverter = new VariantShortConverter();
          } else if (width == 32) {
            typedConverter = new VariantIntConverter();
          } else if (width == 64) {
            typedConverter = new VariantLongConverter();
          } else {
            throw new UnsupportedOperationException("Unsupported shredded value type: " +
                intAnnotation.toString());
          }
        } else if (annotation == null && primitiveType == INT32) {
          typedConverter = new VariantIntConverter();
        } else if (annotation == null && primitiveType == INT64) {
          typedConverter = new VariantLongConverter();
        } else if (primitiveType == FLOAT) {
          typedConverter = new VariantFloatConverter();
        } else if (primitiveType == DOUBLE) {
          typedConverter = new VariantDoubleConverter();
        } else if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
          LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
              (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) annotation;
          typedConverter = new VariantDecimalConverter(decimalType.getScale());
        } else if (annotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
          typedConverter = new VariantDateConverter();
        } else if (annotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
          LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampType =
              (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) annotation;
          if (timestampType.isAdjustedToUTC()) {
            switch (timestampType.getUnit()) {
              case MILLIS:
                throw new IllegalArgumentException("MILLIS not supported by Variant");
              case MICROS:
                typedConverter = new VariantTimestampConverter();
                break;
              case NANOS:
                typedConverter = new VariantTimestampNanosConverter();
                break;
            }
          } else {
            switch (timestampType.getUnit()) {
              case MILLIS:
                throw new IllegalArgumentException("MILLIS not supported by Variant");
              case MICROS:
                typedConverter = new VariantTimestampNtzConverter();
                break;
              case NANOS:
                typedConverter = new VariantTimestampNanosNtzConverter();
                break;
            }
          }
        } else if (annotation instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
          LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeType =
              (LogicalTypeAnnotation.TimeLogicalTypeAnnotation) annotation;
          if (timeType.isAdjustedToUTC() || timeType.getUnit() != MICROS) {
            throw new IllegalArgumentException(timeType.toString() + " not supported by Variant");
          } else {
            typedConverter = new VariantTimeConverter();
          }
        } else if (annotation instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
          typedConverter = new VariantUuidConverter();
        } else if (annotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
          typedConverter = new VariantStringConverter();
        } else if (primitiveType == BINARY) {
          typedConverter = new VariantBinaryConverter();
        } else {
          String annotationString = annotation == null ? "" : annotation.toString();
          throw new UnsupportedOperationException(
              "Unsupported shredded value type: " + field.toString());
        }
      }

      assert (typedConverter != null);
      converters[typedValueIdx] = typedConverter;
    }
  }

  /**
   * called at initialization based on schema
   * must consistently return the same object
   *
   * @param fieldIndex index of the field in this group
   * @return the corresponding converter
   */
  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  /** runtime calls  **/

  /**
   * called at the beginning of the group managed by this converter
   */
  @Override
  public void start() {
    startWritePos = builder.builder.getWritePos();
    if (valueIdx >= 0) {
      ((VariantValueConverter) converters[valueIdx]).reset();
    }
  }

  /**
   * call at the end of the group
   */
  @Override
  public void end() {
    VariantBuilder builder = this.builder.builder;

    Binary variantValue = null;
    if (valueIdx >= 0) {
      variantValue = ((VariantValueConverter) converters[valueIdx]).getValue();
    }
    if (variantValue != null) {
      if (startWritePos == builder.getWritePos()) {
        // Nothing else was added. We can directly append this value.
        builder.shallowAppendVariant(
            variantValue.toByteBuffer().array(),
            variantValue.toByteBuffer().position());
      } else {
        // Both value and typed_value were non-null. This is only valid for an object.
        byte[] value = variantValue.getBytes();
        int basicType = value[0] & VariantUtil.BASIC_TYPE_MASK;
        if (basicType != VariantUtil.OBJECT || !typedValueIsObject) {
          throw new IllegalArgumentException("Invalid variant, conflicting value and typed_value");
        }
        // Write the remaining fields from `value`.
        ArrayList<VariantBuilder.FieldEntry> fields =
            ((VariantObjectConverter) converters[typedValueIdx]).getFields();

        VariantUtil.handleObject(value, 0, (info) -> {
          for (int i = 0; i < info.numElements; ++i) {
            int id = VariantUtil.readUnsigned(value, info.idStart + info.idSize * i, info.idSize);
            int offset = VariantUtil.readUnsigned(
                value, info.offsetStart + info.offsetSize * i, info.offsetSize);
            int elementPos = info.dataStart + offset;
            String key = VariantUtil.getMetadataKey(this.builder.getMetadata().getBytes(), id);
            fields.add(new VariantBuilder.FieldEntry(key, id, builder.getWritePos() - startWritePos));
            builder.shallowAppendVariant(value, elementPos);
          }
          return null;
        });
        builder.finishWritingObject(startWritePos, fields);
      }
    } else if (typedValueIsObject && startWritePos != builder.getWritePos()) {
      // We wrote an object, and there's nothing left to append.
      ArrayList<VariantBuilder.FieldEntry> fields =
          ((VariantObjectConverter) converters[typedValueIdx]).getFields();

      builder.finishWritingObject(startWritePos, fields);
    }

    if (startWritePos == builder.getWritePos() && objectFieldName == null) {
      // If startWritePos == builder.getWritePos(), and this is an array element or top-level field, the
      // spec considers this invalid, but suggests writing a VariantNull to the resulting variant. Given that
      // this is a reference implementation, we might consder failing with an error?
      builder.appendNull();
    }

    if (startWritePos != builder.getWritePos() && objectFieldName != null) {
      if (objectFieldId == -1) {
        // metadata isn't available in the constructor, so we look up the field lazily.
        objectFieldId = builder.addKey(objectFieldName);
      }
      // Record that we added a field.
      parent.addField(new VariantBuilder.FieldEntry(objectFieldName, objectFieldId, startWritePos));
    }
  }
}

/**
 * Converter for shredded Variant values.
 *
 */
public abstract class VariantColumnConverter extends VariantElementConverter {

  private int topLevelMetadataIdx = -1;

  public VariantColumnConverter(GroupType variantSchema) {
    super(variantSchema);

    List<Type> fields = variantSchema.getFields();
    for (int i = 0; i < fields.size(); i++) {
      Type field = fields.get(i);
      String fieldName = field.getName();
      if (fieldName.equals("metadata")) {
        this.topLevelMetadataIdx = i;
        if (!field.isPrimitive() || field.asPrimitiveType().getPrimitiveTypeName() != BINARY) {
          throw new IllegalArgumentException("Metadata must be a binary value");
        }
      }
    }
    if (topLevelMetadataIdx < 0) {
      throw new IllegalArgumentException("Metadata missing from schema");
    }
    converters[topLevelMetadataIdx] = new VariantMetadataConverter();
    builder = new VariantBuilderHolder();
    init(builder);
  }

  /** runtime calls  **/

  /**
   * Set the final shredded value.
   */
  public abstract void addVariant(Binary value, Binary metadata);

  /**
   * called at the beginning of the group managed by this converter
   */
  @Override
  public void start() {
    builder.startNewVariant();
    super.start();
  }

  /**
   * call at the end of the group
   */
  @Override
  public void end() {
    super.end();
    byte[] value = builder.builder.valueWithoutMetadata();
    addVariant(Binary.fromConstantByteArray(value), builder.getMetadata());
  }
}

/**
 * Converter for the metadata column. It sets the current metadata in the parent converter,
 * so that it can be used by the typed_value converter on the same row.
 */
class VariantMetadataConverter extends PrimitiveConverter implements VariantConverter {
  private VariantBuilderHolder builder;
  Binary[] dict;

  public VariantMetadataConverter() {
    dict = null;
  }

  @Override
  public void init(VariantBuilderHolder builderHolder) {
    builder = builderHolder;
  }

  @Override
  public void addBinary(Binary value) {
    builder.setMetadata(value);
  }

  @Override
  public boolean hasDictionarySupport() {
    return true;
  }

  @Override
  public void setDictionary(Dictionary dictionary) {
    dict = new Binary[dictionary.getMaxId() + 1];
    for (int i = 0; i <= dictionary.getMaxId(); i++) {
      dict[i] = dictionary.decodeToBinary(i);
    }
  }

  @Override
  public void addValueFromDictionary(int dictionaryId) {
    builder.setMetadata(dict[dictionaryId]);
  }
}

class VariantValueConverter extends PrimitiveConverter implements VariantConverter {
  private VariantElementConverter parent;
  Binary[] dict;
  Binary currentValue;

  public VariantValueConverter(VariantElementConverter parent) {
    this.parent = parent;
    this.currentValue = null;
    dict = null;
  }

  // Value reader doesn't need a builder - it just holds onto its value for the parent to consume.
  @Override
  public void init(VariantBuilderHolder builderHolder) {}

  void reset() {
    currentValue = null;
  }

  Binary getValue() {
    return currentValue;
  }

  @Override
  public void addBinary(Binary value) {
    currentValue = value;
  }

  @Override
  public boolean hasDictionarySupport() {
    return true;
  }

  @Override
  public void setDictionary(Dictionary dictionary) {
    dict = new Binary[dictionary.getMaxId() + 1];
    for (int i = 0; i <= dictionary.getMaxId(); i++) {
      dict[i] = dictionary.decodeToBinary(i);
    }
  }

  @Override
  public void addValueFromDictionary(int dictionaryId) {
    currentValue = dict[dictionaryId];
  }
}

class VariantScalarConverter extends PrimitiveConverter implements VariantConverter {
  protected VariantBuilderHolder builder;
  private GroupType scalarType;

  @Override
  public void init(VariantBuilderHolder builderHolder) {
    builder = builderHolder;
  }
}

class VariantStringConverter extends VariantScalarConverter {
  @Override
  public void addBinary(Binary value) {
    builder.builder.appendString(value.toStringUsingUTF8());
  }
}

class VariantBinaryConverter extends VariantScalarConverter {
  @Override
  public void addBinary(Binary value) {
    builder.builder.appendBinary(value.getBytes());
  }
}

class VariantDecimalConverter extends VariantScalarConverter {
  private int scale;

  VariantDecimalConverter(int scale) {
    super();
    this.scale = scale;
  }

  @Override
  public void addBinary(Binary value) {
    builder.builder.appendDecimal(
        new BigDecimal(new BigInteger(value.getBytes()), scale));
  }

  @Override
  public void addLong(long value) {
    BigDecimal decimal = BigDecimal.valueOf(value, scale);
    builder.builder.appendDecimal(decimal);
  }

  @Override
  public void addInt(int value) {
    BigDecimal decimal = BigDecimal.valueOf(value, scale);
    builder.builder.appendDecimal(decimal);
  }
}

class VariantUuidConverter extends VariantScalarConverter {
  @Override
  public void addBinary(Binary value) {
    builder.builder.appendUUIDBytes(value.getBytes());
  }
}

class VariantBooleanConverter extends VariantScalarConverter {
  @Override
  public void addBoolean(boolean value) {
    builder.builder.appendBoolean(value);
  }
}

class VariantDoubleConverter extends VariantScalarConverter {
  @Override
  public void addDouble(double value) {
    builder.builder.appendDouble(value);
  }
}

class VariantFloatConverter extends VariantScalarConverter {
  @Override
  public void addFloat(float value) {
    builder.builder.appendFloat(value);
  }
}

class VariantByteConverter extends VariantScalarConverter {
  @Override
  public void addInt(int value) {
    // TODO: Fix
    builder.builder.appendLong(value);
  }
}

class VariantShortConverter extends VariantScalarConverter {
  @Override
  public void addInt(int value) {
    // TODO: Fix
    builder.builder.appendLong(value);
  }
}

class VariantIntConverter extends VariantScalarConverter {
  @Override
  public void addInt(int value) {
    // TODO: Fix
    builder.builder.appendLong(value);
  }
}

class VariantLongConverter extends VariantScalarConverter {
  @Override
  public void addLong(long value) {
    builder.builder.appendLong(value);
  }
}

class VariantDateConverter extends VariantScalarConverter {
  @Override
  public void addInt(int value) {
    builder.builder.appendDate(value);
  }
}

class VariantTimeConverter extends VariantScalarConverter {
  @Override
  public void addLong(long value) {
    builder.builder.appendTime(value);
  }
}

class VariantTimestampConverter extends VariantScalarConverter {
  @Override
  public void addLong(long value) {
    builder.builder.appendTimestamp(value);
  }
}

class VariantTimestampNtzConverter extends VariantScalarConverter {
  @Override
  public void addLong(long value) {
    builder.builder.appendTimestampNtz(value);
  }
}

class VariantTimestampNanosConverter extends VariantScalarConverter {
  @Override
  public void addLong(long value) {
    builder.builder.appendTimestampNanos(value);
  }
}

class VariantTimestampNanosNtzConverter extends VariantScalarConverter {
  @Override
  public void addLong(long value) {
    builder.builder.appendTimestampNanosNtz(value);
  }
}

/**
 * Converter for a LIST typed_value.
 */
class VariantArrayConverter extends GroupConverter implements VariantConverter {
  private VariantBuilderHolder builder;
  private VariantArrayRepeatedConverter repeatedConverter;
  private ArrayList<Integer> offsets;
  private int startPos;

  public VariantArrayConverter(GroupType listType) {
    if (listType.getFieldCount() != 1) {
      throw new IllegalArgumentException("LIST must have one field");
    }
    Type middleLevel = listType.getType(0);
    if (!middleLevel.isRepetition(REPEATED)
        || middleLevel.isPrimitive()
        || middleLevel.asGroupType().getFieldCount() != 1) {
      throw new IllegalArgumentException("LIST must have one repeated field");
    }
    this.repeatedConverter = new VariantArrayRepeatedConverter(middleLevel.asGroupType(), this);
  }

  @Override
  public void init(VariantBuilderHolder builderHolder) {
    builder = builderHolder;
    repeatedConverter.init(builderHolder);
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return repeatedConverter;
  }

  public void addElement() {
    offsets.add(builder.builder.getWritePos() - startPos);
  }

  @Override
  public void start() {
    offsets = new ArrayList<>();
    startPos = builder.builder.getWritePos();
  }

  @Override
  public void end() {
    builder.builder.finishWritingArray(startPos, offsets);
  }
}

/**
 * Converter for the repeated field of a LIST typed_value.
 */
class VariantArrayRepeatedConverter extends GroupConverter implements VariantConverter {
  private VariantElementConverter elementConverter;
  private VariantArrayConverter parentConverter;

  public VariantArrayRepeatedConverter(GroupType repeatedType, VariantArrayConverter parentaConverter) {
    this.elementConverter = new VariantElementConverter(repeatedType.getType(0).asGroupType());
    this.parentConverter = parentaConverter;
  }

  @Override
  public void init(VariantBuilderHolder builderHolder) {
    elementConverter.init(builderHolder);
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return elementConverter;
  }

  @Override
  public void start() {
    // Record the offset of this element in the binary.
    parentConverter.addElement();
  }

  @Override
  public void end() {
  }
}

class VariantObjectConverter extends GroupConverter implements VariantConverter {
  private VariantBuilderHolder builder;
  private VariantElementConverter[] converters;
  private ArrayList<VariantBuilder.FieldEntry> fieldEntries = new ArrayList<>();

  public VariantObjectConverter(GroupType typed_value) {
    List<Type> fields = typed_value.getFields();
    converters = new VariantElementConverter[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      GroupType field = fields.get(i).asGroupType();
      String name = fields.get(i).getName();
      converters[i] = new VariantElementConverter(field, name, this);
    }
  };

  void addField(VariantBuilder.FieldEntry entry) {
    fieldEntries.add(entry);
  }

  // Return fieldEntries. May only be called once.
  ArrayList<VariantBuilder.FieldEntry> getFields() {
    return fieldEntries;
  }

  @Override
  public void init(VariantBuilderHolder builderHolder) {
    builder = builderHolder;
    for (VariantElementConverter c: converters) {
      c.init(builderHolder);
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
    fieldEntries.clear();
  }

  @Override
  public void end() {
    // We can't finish writing the object here, because there might be residual entries in our
    // parent's value column. The parent converter calls getFields to finalize the object.
  }
}

