package com.ebay.chluo.hdfs.client;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSClient.DFSInputStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DFSClientTest {
   private static DFSClient client = null;

   @BeforeClass
   public static void setUpBeforeClass() throws Exception {
      client = new DFSClient(new Configuration());
   }

   @AfterClass
   public static void tearDownAfterClass() throws Exception {
      client.close();
   }

   @Test
   public void testClient() {
      String path = "/test/a.txt";
      String content = "hello world";
      try {
         OutputStream out = client.create(path, true);
         BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
         for (int i = 0; i < 10000; i++) {
            writer.write(content);
         }
         writer.close();

         assertTrue(client.exists(path));

         DFSInputStream in = client.open(path);
         BufferedReader reader = new BufferedReader(new InputStreamReader(in));
         String result = reader.readLine();
         assertEquals(content, result);
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

}
