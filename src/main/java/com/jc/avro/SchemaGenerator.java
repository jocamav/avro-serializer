package com.jc.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class SchemaGenerator {

    public static Schema createAvroHttpRequestSchema(){

        Schema clientIdentifier = SchemaBuilder.record("ClientIdentifier").namespace("com.jc.avro.model")
                .fields().requiredString("hostName").requiredString("ipAddress").endRecord();

        Schema avroHttpRequest = SchemaBuilder.record("AvroHttpRequest").namespace("com.jc.avro.model").fields()
                .requiredLong("requestTime")
                .name("identifier").type(clientIdentifier).noDefault()
                .name("employeeNames").type().array().items().stringType().arrayDefault(null)
                .name("active").type().enumeration("Active").symbols("YES", "NO").noDefault()
                .endRecord();
        return avroHttpRequest;
    }

}
