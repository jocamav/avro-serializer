package com.jc;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import com.jc.avro.SchemaGenerator;
import com.jc.avro.model.User;


public class AvroTest {

    @Test
    public void testSerializer() throws IOException {
        DataFileWriter<User> dataFileWriter = null;
        File file = new File("users.avro");
        try {
            User user1 = new User();
            user1.setName("Alyssa");
            user1.setFavoriteNumber(256);
            // Leave favorite color null

            // Alternate constructor
            User user2 = new User("Ben", 7, "red");

            // Construct via builder
            User user3 = User.newBuilder()
                    .setName("Charlie")
                    .setFavoriteColor("blue")
                    .setFavoriteNumber(null)
                    .build();

            // Serialize user1, user2 and user3 to disk
            DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);

            dataFileWriter = new DataFileWriter<User>(userDatumWriter);
            dataFileWriter.create(user1.getSchema(), file);
            dataFileWriter.append(user1);
            dataFileWriter.append(user2);
            dataFileWriter.append(user3);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if(dataFileWriter != null) {
                dataFileWriter.close();
            }
        }

        // Deserialize Users from disk
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<User>(file, userDatumReader);
        User user = null;
        while (dataFileReader.hasNext()) {
            // Reuse user object by passing it to next(). This saves us from
            // allocating and garbage collecting many objects for files with
            // many items.
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }

    @Test
    public void generateSchema() throws IOException {
        Schema schema = SchemaGenerator.createAvroHttpRequestSchema();
        PrintWriter out = new PrintWriter("src/main/avro/schema.avsc");
        out.write(schema.toString());
        out.close();
        System.out.println(schema.toString());
    }
}
