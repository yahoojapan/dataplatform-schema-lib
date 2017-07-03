/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.dataplatform.schema.formatter;

import jp.co.yahoo.dataplatform.schema.parser.AvroMessageReader;
import jp.co.yahoo.dataplatform.schema.parser.IParser;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class TestAvroMessageWriter{

  //---------------------------------------
  // DataProvider
  //---------------------------------------

  @DataProvider( name = "testcase" )
  public Object[][] testcase(){
    return new Object[][]{
      {
        "{\"namespace\": \"example.avro\",\n" +
          " \"type\": \"array\",\n" +
          " \"items\": \"int\"\n" +
          "}\n"
      }
    };
  }

  @DataProvider( name = "testcase_create" )
  public Object[][] testcase_crate() throws UnsupportedEncodingException{
    final String FIELDS_MESSAGE_VERSION = "message_version";
    final String FIELDS_SCHEMA_NAME = "schema_name";
    final String FIELDS_SCHEMA_VERSION = "schema_version";
    final String FIELDS_DEDUPLICATION_ID = "deduplication_id";
    final String FIELDS_PROJECT = "project";
    final String FIELDS_DATASET = "dataset";
    final String FIELDS_PARTITIONING_TIME = "partitioning_time";
    final String FIELDS_BODY = "body";
    final String FIELDS_SENDER = "sender";
    final String VERSION = "1.1.0";

    return new Object[][]{
      {
        "{\"namespace\": \"jp.co.yahoo.data.schema\",\n" +
          " \"type\": \"record\",\n" +
          " \"name\": \"Message\",\n" +
          " \"fields\": [\n" +
          "     {\"name\": \"message_version\", \"type\": \"string\"},\n" +
          "     {\"name\": \"schema_name\", \"type\": \"string\"},\n" +
          "     {\"name\": \"schema_version\", \"type\": \"string\"},\n" +
          "     {\"name\": \"deduplication_id\",  \"type\": [\"null\", \"string\"]},\n" +
          "     {\"name\": \"project\", \"type\": \"string\"},\n" +
          "     {\"name\": \"dataset\", \"type\": \"string\"},\n" +
          "     {\"name\": \"partitioning_time\", \"type\": \"long\"},\n" +
          "     {\"name\": \"sender\", \"type\": \"string\"},\n" +
          "     {\"name\": \"body\", \"type\": \"bytes\"}\n" +
          " ]\n" +
          "}" ,
        new HashMap<Object,Object>(){
          {
            put( FIELDS_MESSAGE_VERSION, "1.1.0" );
            put( FIELDS_SCHEMA_NAME, "test_schema" );
            put( FIELDS_SCHEMA_VERSION, "2.2.2" );
            put( FIELDS_DEDUPLICATION_ID, "test_deduplication_id" );
            put( FIELDS_PROJECT, "test_project" );
            put( FIELDS_DATASET, "test_dataset" );
            put( FIELDS_PARTITIONING_TIME, ( long ) 100000000 );
            put( FIELDS_SENDER, "test_sender" );
            put( FIELDS_BODY, "hoge".getBytes( "UTF-8" ) );
          }
        } ,
        new HashMap<Object,String>(){
          {
            put( FIELDS_MESSAGE_VERSION, "1.1.0" );
            put( FIELDS_SCHEMA_NAME, "test_schema" );
            put( FIELDS_SCHEMA_VERSION, "2.2.2" );
            put( FIELDS_DEDUPLICATION_ID, "test_deduplication_id" );
            put( FIELDS_PROJECT, "test_project" );
            put( FIELDS_DATASET, "test_dataset" );
            put( FIELDS_PARTITIONING_TIME, "100000000" );
            put( FIELDS_SENDER, "test_sender" );
            put( FIELDS_BODY, "hoge" );
          }
        }
      }
    };
  }

  //---------------------------------------
  // Test
  //---------------------------------------

  @Test( dataProvider = "testcase_create" )
  public void T_create( final String inputSchemaString, Map<Object,Object> inputMap, Map<Object,String> expectedMap ){
    try{
      AvroMessageWriter avroMessageWriter = new AvroMessageWriter( inputSchemaString );
      byte[] message = avroMessageWriter.create( inputMap );

      AvroMessageReader reader = new AvroMessageReader( inputSchemaString );
      IParser parser = reader.create( message );

      for( String key : parser.getAllKey() ){
        assertEquals( parser.get( key ).getString(), expectedMap.get( key ) );
      }
    }catch( IOException e ){
      e.printStackTrace();
    }
  }

}
