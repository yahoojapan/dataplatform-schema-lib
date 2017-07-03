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
package jp.co.yahoo.dataplatform.schema.parser;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import static org.testng.Assert.assertEquals;

import jp.co.yahoo.dataplatform.schema.formatter.AvroMessageWriter;
import jp.co.yahoo.dataplatform.schema.formatter.AvroStreamWriter;
import jp.co.yahoo.dataplatform.schema.formatter.IMessageWriter;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;
import jp.co.yahoo.dataplatform.schema.objects.StringObj;
import org.testng.annotations.Test;

import jp.co.yahoo.dataplatform.schema.formatter.IStreamWriter;

public class TestAvroStreamReader {

  @Test
  public void T_example_record() throws IOException{
    String fileName = this.getClass().getClassLoader().getResource( "schema/record.avsc" ).getPath();

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    IStreamWriter writer = new AvroStreamWriter( out , new File( fileName ) );

    Map<Object,Object> dataContainer = new HashMap<Object,Object>();
    dataContainer.put("name", "Alyssa");
    dataContainer.put("favorite_number", 256);

    writer.write( dataContainer );

    dataContainer.clear();

    dataContainer.put("name", "Ben");
    dataContainer.put("favorite_number", 7);
    dataContainer.put("favorite_color", "red");
    dataContainer.put("f", "red");

    writer.write( dataContainer );
    writer.close();

    byte[] avroBytes = out.toByteArray();

    ByteArrayInputStream in = new ByteArrayInputStream( avroBytes );
    IStreamReader reader = new AvroStreamReader( in , new File( fileName ) );

    while( reader.hasNext() ){
      IParser parser = reader.next();
      PrimitiveObject name = parser.get( "name" );
      System.out.println( name.getString() );
      PrimitiveObject favNumber = parser.get( "favorite_number" );
      System.out.println( favNumber.getInt() );
      PrimitiveObject favNumber2 = parser.get( "f" );
      System.out.println( favNumber2.getString() );
    }
  }

  @Test
  public void T_example_array() throws IOException{
    String fileName = this.getClass().getClassLoader().getResource( "schema/array.avsc" ).getPath();

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    IStreamWriter writer = new AvroStreamWriter( out , new File( fileName ) );

    List<Object> dataContainer = new ArrayList<Object>();
    dataContainer.add( 100 );
    dataContainer.add( 200 );
    dataContainer.add( 300 );

    writer.write( dataContainer );

    dataContainer.clear();

    dataContainer.add( 100 );
    dataContainer.add( 200 );
    dataContainer.add( 300 );
    dataContainer.add( 400 );
    dataContainer.add( 500 );
    dataContainer.add( new StringObj( "600" ) );

    writer.write( dataContainer );
    writer.close();

    byte[] avroBytes = out.toByteArray();

    ByteArrayInputStream in = new ByteArrayInputStream( avroBytes );
    IStreamReader reader = new AvroStreamReader( in , new File( fileName ) );

    while( reader.hasNext() ){
      IParser parser = reader.next();
      for( int i = 0 ; i < parser.size() ; i++ ){
        System.out.println( parser.get(i).getString() );
      }
    }
  }

  @Test
  public void T_example_nested() throws IOException{
    String fileName = this.getClass().getClassLoader().getResource( "schema/nested_example.avsc" ).getPath();

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    IStreamWriter writer = new AvroStreamWriter( out , new File( fileName ) );

    Map<Object,Object> dataContainer = new HashMap<Object,Object>();
    dataContainer.put("name", "Alyssa");

    Map<Object,Object> mapContainer = new HashMap<Object,Object>();
    mapContainer.put( "MAP_1" , "_1" );
    mapContainer.put( "MAP_2" , "_2" );
    dataContainer.put( "map_value" , mapContainer );

    List<Object> arrayContainer = new ArrayList<Object>();
    arrayContainer.add( 100 );
    arrayContainer.add( 10 );
    arrayContainer.add( 1 );
    dataContainer.put( "array_value" , arrayContainer );

    Map<Object,Object> recordContainer = new HashMap<Object,Object>();
    recordContainer.put( "value1" , "a" );
    recordContainer.put( "value2" , "b" );
    recordContainer.put( "value3" , "c" );
    dataContainer.put( "record_value" , recordContainer );

    writer.write( dataContainer );

    writer.close();

    byte[] avroBytes = out.toByteArray();

    ByteArrayInputStream in = new ByteArrayInputStream( avroBytes );
    IStreamReader reader = new AvroStreamReader( in , new File( fileName ) );

    while( reader.hasNext() ){
      IParser parser = reader.next();
      PrimitiveObject name = parser.get( "name" );
      System.out.println( name.getString() );
      PrimitiveObject array = parser.get( "array_value" );
      System.out.println( array.getString() );
      PrimitiveObject favNumber2 = parser.get( "map_value" );
      System.out.println( favNumber2.getString() );
      IParser recordParser = parser.getParser( "record_value" );
      System.out.println( recordParser.get( "value1" ).getString() );
      System.out.println( recordParser.get( "value2" ).getString() );
      System.out.println( recordParser.get( "value3" ).getString() );
    }
  }

  @Test
  public void T_example_nested2() throws IOException{
    String fileName = this.getClass().getClassLoader().getResource( "schema/nested_example.avsc" ).getPath()
;

    IMessageWriter writer = new AvroMessageWriter( new File( fileName ) );

    Map<Object,Object> dataContainer = new HashMap<Object,Object>();
    dataContainer.put("name", "Alyssa");

    Map<Object,Object> mapContainer = new HashMap<Object,Object>();
    mapContainer.put( "MAP_1" , "_1" );
    mapContainer.put( "MAP_2" , "_2" );
    dataContainer.put( "map_value" , mapContainer );

    List<Object> arrayContainer = new ArrayList<Object>();
    arrayContainer.add( 100 );
    arrayContainer.add( 10 );
    arrayContainer.add( 1 );
    dataContainer.put( "array_value" , arrayContainer );

    Map<Object,Object> recordContainer = new HashMap<Object,Object>();
    recordContainer.put( "value1" , "a" );
    recordContainer.put( "value2" , "b" );
    recordContainer.put( "value3" , "c" );
    dataContainer.put( "record_value" , recordContainer );

    byte[] avroBytes = writer.create( dataContainer );

    IMessageReader reader = new AvroMessageReader( new File( fileName ) );
    IParser parser = reader.create( avroBytes );

    PrimitiveObject name = parser.get( "name" );
    System.out.println( name.getString() );
    PrimitiveObject array = parser.get( "array_value" );
    System.out.println( array.getString() );
    PrimitiveObject favNumber2 = parser.get( "map_value" );
    System.out.println( favNumber2.getString() );
    IParser recordParser = parser.getParser( "record_value" );
    System.out.println( recordParser.get( "value1" ).getString() );
    System.out.println( recordParser.get( "value2" ).getString() );
    System.out.println( recordParser.get( "value3" ).getString() );
  }

}
