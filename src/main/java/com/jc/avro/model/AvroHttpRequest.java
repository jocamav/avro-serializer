/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.jc.avro.model;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class AvroHttpRequest extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 6666935897385790739L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"AvroHttpRequest\",\"namespace\":\"com.jc.avro.model\",\"fields\":[{\"name\":\"requestTime\",\"type\":\"long\"},{\"name\":\"identifier\",\"type\":{\"type\":\"record\",\"name\":\"ClientIdentifier\",\"fields\":[{\"name\":\"hostName\",\"type\":\"string\"},{\"name\":\"ipAddress\",\"type\":\"string\"}]}},{\"name\":\"employeeNames\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"default\":null},{\"name\":\"active\",\"type\":{\"type\":\"enum\",\"name\":\"Active\",\"symbols\":[\"YES\",\"NO\"]}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<AvroHttpRequest> ENCODER =
      new BinaryMessageEncoder<AvroHttpRequest>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<AvroHttpRequest> DECODER =
      new BinaryMessageDecoder<AvroHttpRequest>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   */
  public static BinaryMessageDecoder<AvroHttpRequest> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   */
  public static BinaryMessageDecoder<AvroHttpRequest> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<AvroHttpRequest>(MODEL$, SCHEMA$, resolver);
  }

  /** Serializes this AvroHttpRequest to a ByteBuffer. */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /** Deserializes a AvroHttpRequest from a ByteBuffer. */
  public static AvroHttpRequest fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  @Deprecated public long requestTime;
  @Deprecated public com.jc.avro.model.ClientIdentifier identifier;
  @Deprecated public java.util.List<java.lang.CharSequence> employeeNames;
  @Deprecated public com.jc.avro.model.Active active;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public AvroHttpRequest() {}

  /**
   * All-args constructor.
   * @param requestTime The new value for requestTime
   * @param identifier The new value for identifier
   * @param employeeNames The new value for employeeNames
   * @param active The new value for active
   */
  public AvroHttpRequest(java.lang.Long requestTime, com.jc.avro.model.ClientIdentifier identifier, java.util.List<java.lang.CharSequence> employeeNames, com.jc.avro.model.Active active) {
    this.requestTime = requestTime;
    this.identifier = identifier;
    this.employeeNames = employeeNames;
    this.active = active;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return requestTime;
    case 1: return identifier;
    case 2: return employeeNames;
    case 3: return active;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: requestTime = (java.lang.Long)value$; break;
    case 1: identifier = (com.jc.avro.model.ClientIdentifier)value$; break;
    case 2: employeeNames = (java.util.List<java.lang.CharSequence>)value$; break;
    case 3: active = (com.jc.avro.model.Active)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'requestTime' field.
   * @return The value of the 'requestTime' field.
   */
  public java.lang.Long getRequestTime() {
    return requestTime;
  }

  /**
   * Sets the value of the 'requestTime' field.
   * @param value the value to set.
   */
  public void setRequestTime(java.lang.Long value) {
    this.requestTime = value;
  }

  /**
   * Gets the value of the 'identifier' field.
   * @return The value of the 'identifier' field.
   */
  public com.jc.avro.model.ClientIdentifier getIdentifier() {
    return identifier;
  }

  /**
   * Sets the value of the 'identifier' field.
   * @param value the value to set.
   */
  public void setIdentifier(com.jc.avro.model.ClientIdentifier value) {
    this.identifier = value;
  }

  /**
   * Gets the value of the 'employeeNames' field.
   * @return The value of the 'employeeNames' field.
   */
  public java.util.List<java.lang.CharSequence> getEmployeeNames() {
    return employeeNames;
  }

  /**
   * Sets the value of the 'employeeNames' field.
   * @param value the value to set.
   */
  public void setEmployeeNames(java.util.List<java.lang.CharSequence> value) {
    this.employeeNames = value;
  }

  /**
   * Gets the value of the 'active' field.
   * @return The value of the 'active' field.
   */
  public com.jc.avro.model.Active getActive() {
    return active;
  }

  /**
   * Sets the value of the 'active' field.
   * @param value the value to set.
   */
  public void setActive(com.jc.avro.model.Active value) {
    this.active = value;
  }

  /**
   * Creates a new AvroHttpRequest RecordBuilder.
   * @return A new AvroHttpRequest RecordBuilder
   */
  public static com.jc.avro.model.AvroHttpRequest.Builder newBuilder() {
    return new com.jc.avro.model.AvroHttpRequest.Builder();
  }

  /**
   * Creates a new AvroHttpRequest RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new AvroHttpRequest RecordBuilder
   */
  public static com.jc.avro.model.AvroHttpRequest.Builder newBuilder(com.jc.avro.model.AvroHttpRequest.Builder other) {
    return new com.jc.avro.model.AvroHttpRequest.Builder(other);
  }

  /**
   * Creates a new AvroHttpRequest RecordBuilder by copying an existing AvroHttpRequest instance.
   * @param other The existing instance to copy.
   * @return A new AvroHttpRequest RecordBuilder
   */
  public static com.jc.avro.model.AvroHttpRequest.Builder newBuilder(com.jc.avro.model.AvroHttpRequest other) {
    return new com.jc.avro.model.AvroHttpRequest.Builder(other);
  }

  /**
   * RecordBuilder for AvroHttpRequest instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<AvroHttpRequest>
    implements org.apache.avro.data.RecordBuilder<AvroHttpRequest> {

    private long requestTime;
    private com.jc.avro.model.ClientIdentifier identifier;
    private com.jc.avro.model.ClientIdentifier.Builder identifierBuilder;
    private java.util.List<java.lang.CharSequence> employeeNames;
    private com.jc.avro.model.Active active;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(com.jc.avro.model.AvroHttpRequest.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.requestTime)) {
        this.requestTime = data().deepCopy(fields()[0].schema(), other.requestTime);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.identifier)) {
        this.identifier = data().deepCopy(fields()[1].schema(), other.identifier);
        fieldSetFlags()[1] = true;
      }
      if (other.hasIdentifierBuilder()) {
        this.identifierBuilder = com.jc.avro.model.ClientIdentifier.newBuilder(other.getIdentifierBuilder());
      }
      if (isValidValue(fields()[2], other.employeeNames)) {
        this.employeeNames = data().deepCopy(fields()[2].schema(), other.employeeNames);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.active)) {
        this.active = data().deepCopy(fields()[3].schema(), other.active);
        fieldSetFlags()[3] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing AvroHttpRequest instance
     * @param other The existing instance to copy.
     */
    private Builder(com.jc.avro.model.AvroHttpRequest other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.requestTime)) {
        this.requestTime = data().deepCopy(fields()[0].schema(), other.requestTime);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.identifier)) {
        this.identifier = data().deepCopy(fields()[1].schema(), other.identifier);
        fieldSetFlags()[1] = true;
      }
      this.identifierBuilder = null;
      if (isValidValue(fields()[2], other.employeeNames)) {
        this.employeeNames = data().deepCopy(fields()[2].schema(), other.employeeNames);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.active)) {
        this.active = data().deepCopy(fields()[3].schema(), other.active);
        fieldSetFlags()[3] = true;
      }
    }

    /**
      * Gets the value of the 'requestTime' field.
      * @return The value.
      */
    public java.lang.Long getRequestTime() {
      return requestTime;
    }

    /**
      * Sets the value of the 'requestTime' field.
      * @param value The value of 'requestTime'.
      * @return This builder.
      */
    public com.jc.avro.model.AvroHttpRequest.Builder setRequestTime(long value) {
      validate(fields()[0], value);
      this.requestTime = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'requestTime' field has been set.
      * @return True if the 'requestTime' field has been set, false otherwise.
      */
    public boolean hasRequestTime() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'requestTime' field.
      * @return This builder.
      */
    public com.jc.avro.model.AvroHttpRequest.Builder clearRequestTime() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'identifier' field.
      * @return The value.
      */
    public com.jc.avro.model.ClientIdentifier getIdentifier() {
      return identifier;
    }

    /**
      * Sets the value of the 'identifier' field.
      * @param value The value of 'identifier'.
      * @return This builder.
      */
    public com.jc.avro.model.AvroHttpRequest.Builder setIdentifier(com.jc.avro.model.ClientIdentifier value) {
      validate(fields()[1], value);
      this.identifierBuilder = null;
      this.identifier = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'identifier' field has been set.
      * @return True if the 'identifier' field has been set, false otherwise.
      */
    public boolean hasIdentifier() {
      return fieldSetFlags()[1];
    }

    /**
     * Gets the Builder instance for the 'identifier' field and creates one if it doesn't exist yet.
     * @return This builder.
     */
    public com.jc.avro.model.ClientIdentifier.Builder getIdentifierBuilder() {
      if (identifierBuilder == null) {
        if (hasIdentifier()) {
          setIdentifierBuilder(com.jc.avro.model.ClientIdentifier.newBuilder(identifier));
        } else {
          setIdentifierBuilder(com.jc.avro.model.ClientIdentifier.newBuilder());
        }
      }
      return identifierBuilder;
    }

    /**
     * Sets the Builder instance for the 'identifier' field
     * @param value The builder instance that must be set.
     * @return This builder.
     */
    public com.jc.avro.model.AvroHttpRequest.Builder setIdentifierBuilder(com.jc.avro.model.ClientIdentifier.Builder value) {
      clearIdentifier();
      identifierBuilder = value;
      return this;
    }

    /**
     * Checks whether the 'identifier' field has an active Builder instance
     * @return True if the 'identifier' field has an active Builder instance
     */
    public boolean hasIdentifierBuilder() {
      return identifierBuilder != null;
    }

    /**
      * Clears the value of the 'identifier' field.
      * @return This builder.
      */
    public com.jc.avro.model.AvroHttpRequest.Builder clearIdentifier() {
      identifier = null;
      identifierBuilder = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'employeeNames' field.
      * @return The value.
      */
    public java.util.List<java.lang.CharSequence> getEmployeeNames() {
      return employeeNames;
    }

    /**
      * Sets the value of the 'employeeNames' field.
      * @param value The value of 'employeeNames'.
      * @return This builder.
      */
    public com.jc.avro.model.AvroHttpRequest.Builder setEmployeeNames(java.util.List<java.lang.CharSequence> value) {
      validate(fields()[2], value);
      this.employeeNames = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'employeeNames' field has been set.
      * @return True if the 'employeeNames' field has been set, false otherwise.
      */
    public boolean hasEmployeeNames() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'employeeNames' field.
      * @return This builder.
      */
    public com.jc.avro.model.AvroHttpRequest.Builder clearEmployeeNames() {
      employeeNames = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'active' field.
      * @return The value.
      */
    public com.jc.avro.model.Active getActive() {
      return active;
    }

    /**
      * Sets the value of the 'active' field.
      * @param value The value of 'active'.
      * @return This builder.
      */
    public com.jc.avro.model.AvroHttpRequest.Builder setActive(com.jc.avro.model.Active value) {
      validate(fields()[3], value);
      this.active = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'active' field has been set.
      * @return True if the 'active' field has been set, false otherwise.
      */
    public boolean hasActive() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'active' field.
      * @return This builder.
      */
    public com.jc.avro.model.AvroHttpRequest.Builder clearActive() {
      active = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public AvroHttpRequest build() {
      try {
        AvroHttpRequest record = new AvroHttpRequest();
        record.requestTime = fieldSetFlags()[0] ? this.requestTime : (java.lang.Long) defaultValue(fields()[0]);
        if (identifierBuilder != null) {
          record.identifier = this.identifierBuilder.build();
        } else {
          record.identifier = fieldSetFlags()[1] ? this.identifier : (com.jc.avro.model.ClientIdentifier) defaultValue(fields()[1]);
        }
        record.employeeNames = fieldSetFlags()[2] ? this.employeeNames : (java.util.List<java.lang.CharSequence>) defaultValue(fields()[2]);
        record.active = fieldSetFlags()[3] ? this.active : (com.jc.avro.model.Active) defaultValue(fields()[3]);
        return record;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<AvroHttpRequest>
    WRITER$ = (org.apache.avro.io.DatumWriter<AvroHttpRequest>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<AvroHttpRequest>
    READER$ = (org.apache.avro.io.DatumReader<AvroHttpRequest>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
