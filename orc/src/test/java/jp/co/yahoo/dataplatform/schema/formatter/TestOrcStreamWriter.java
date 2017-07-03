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

import java.io.IOException;
import java.io.File;

import java.util.*;

import jp.co.yahoo.dataplatform.schema.design.*;
import jp.co.yahoo.dataplatform.schema.parser.IParser;
import jp.co.yahoo.dataplatform.schema.parser.IStreamReader;
import jp.co.yahoo.dataplatform.schema.parser.OrcStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.*;

public class TestOrcStreamWriter{
  private static final String COL1 = "col1";
  private static final String COL2 = "col2";
  private static final String COL3 = "col3";
  private static final String COL4 = "col4";
  private static final String COL5 = "col5";
  private static final String COL6 = "col6";
  private static final String FOO_KEY = "foo";
  private static final String BAR_KEY = "bar";
  private static final String COL_TEST = "col_test";
  private static final String VAL1 = "aaaa";
  private static final int VAL2 = 2;
  private static final long VAL3 = Long.MAX_VALUE;
  private final List<String> val4 = new ArrayList<>();
  private static final String VAL4_1 = "val4_1";
  private static final String VAL4_2 = "val4_2";
  private static final String VAL4_3 = "val4_3";
  private final Map<String, Integer> val5 = new HashMap<>();
  private static final String KEY5_1 = "key5_1";
  private static final String KEY5_2 = "key5_2";
  private static final int VAL5_1 = 1;
  private static final int VAL5_2 = 2;
  private final Map<Object, Object> val6 = new HashMap<>();
  private static final String FOO_VAL = "foo_val";
  private static final long BAR_VAL = 1000;
  private static final String VAL_TEST = "val_test";
  private static final String OUTPUT_FILE_PATH = "/tmp/orc_stream_writer_test.orc";
  private final Configuration configuration = new Configuration();
  private final static String SCHEMA = "struct<col1:string,col2:int,col3:bigint,col4:array<string>,col5:map<string,int>,col6:struct<foo:string,bar:bigint>>";

  public TestOrcStreamWriter(){
    // set values
    val4.add(VAL4_1);
    val4.add(VAL4_2);
    val4.add(VAL4_3);

    val5.put(KEY5_1, VAL5_1);
    val5.put(KEY5_2, VAL5_2);

    val6.put(FOO_KEY, FOO_VAL);
    val6.put(BAR_KEY, BAR_VAL);
    configuration.set( "fs.default.name" , "file:///" );
  }

  private IField getGeneralField() throws IOException{
    StructContainerField structContainerField = new StructContainerField("root");
    structContainerField.set(new StringField(COL1));
    structContainerField.set(new IntegerField(COL2));
    structContainerField.set(new LongField(COL3));
    structContainerField.set(new ArrayContainerField(COL4, new StringField("array")));
    structContainerField.set(new MapContainerField(COL5, new NullField("map_init")));
    StructContainerField col6 = new StructContainerField(COL6);
    col6.set(new StringField(FOO_KEY));
    col6.set(new LongField(BAR_KEY));
    structContainerField.set(col6);

    return structContainerField;
  }

  private void delete(final File file){
    System.out.println("delete file" + file.getName());
   if( file.exists() ){
      file.delete();
    }
  }

  private void testCol4(final IParser parser) throws IOException{
    IParser valParser = parser.getParser(COL4);
    for(int index = 0; index < val4.size(); index++){
      assertEquals( valParser.get(index).getString(), val4.get(index));
    }
  }

  private void testCol5(final IParser parser) throws IOException{
    IParser valParser = parser.getParser(COL5);
    for (String key: valParser.getAllKey()) {
      assertEquals(valParser.get(key).getInt(), val5.get(key).intValue());
    }
  }

  private void testCol6(final IParser parser) throws IOException {
    IParser valParser = parser.getParser(COL6);
    assertEquals(valParser.get(FOO_KEY).getString(), FOO_VAL);
    assertEquals(valParser.get(BAR_KEY).getLong(), BAR_VAL);
  }

  @BeforeMethod(alwaysRun = true)
  public void setup(){
    File file = new File( OUTPUT_FILE_PATH );
    delete(file);
  }

  @AfterMethod(alwaysRun = true)
  public void creanup(){
    File file = new File( OUTPUT_FILE_PATH );
    delete(file);
  }


  @DataProvider(name = "not_enough_data")
  private Object[][] notEnoughData(){
     Map<Object,Object> data = new HashMap<>();
     data.put( COL1 , VAL1 );
     data.put( COL2 , VAL2 );

     List<Object> dataList = new ArrayList<>();
     dataList.add( VAL1 );
     dataList.add( VAL2 );

     return new Object[][] {{data, dataList}};
  }


  @DataProvider(name = "just_data")
  private Object[][] justData(){
    Map<Object,Object> data = new HashMap<>();
    data.put( COL1 , VAL1 );
    data.put( COL2 , VAL2 );
    data.put( COL3 , VAL3 );
    data.put( COL4 , val4 );
    data.put( COL5 , val5 );
    data.put( COL6 , val6 );

    List<Object> dataList = new ArrayList<>();
    dataList.add( VAL1 );
    dataList.add( VAL2 );
    dataList.add( VAL3 );
    dataList.add( val4 );
    dataList.add( val5 );
    dataList.add( val6 );

    return new Object[][] {{data, dataList}};
  }

  @DataProvider(name = "too_long_data")
  public Object[][] tooLongData(){
    Map<Object,Object> data = new HashMap<>();
    data.put( COL1 , VAL1 );
    data.put( COL2 , VAL2 );
    data.put( COL3 , VAL3 );
    data.put( COL4 , val4 );
    data.put( COL5 , val5 );
    data.put( COL6 , val6 );
    data.put( COL_TEST , VAL_TEST );

    List<Object> dataList = new ArrayList<>();
    dataList.add( VAL1 );
    dataList.add( VAL2 );
    dataList.add( VAL3 );
    dataList.add( val4 );
    dataList.add( val5 );
    dataList.add( val6 );
    dataList.add( VAL_TEST );
    return new Object[][] {{data, dataList}};
  }

  @Test(dataProvider = "not_enough_data")
  public void T_stringSchemaNotEnoughDataTest(final Map<Object, Object> dataMap, final List<Object> dataList) throws IOException{
    Path path = new Path( OUTPUT_FILE_PATH );
    IStreamWriter writer = new OrcStreamWriter( configuration , path , SCHEMA );
    writer.write( dataMap );
    writer.write( dataList );
    writer.close();

    IStreamReader reader = new OrcStreamReader( configuration , path );
    while( reader.hasNext() ) {
      IParser parser = reader.next();
      assertEquals(VAL1, parser.get(COL1).getString());
      assertEquals(VAL2, parser.get(COL2).getInt()) ;
    }
  }

  @Test(dataProvider = "not_enough_data")
  public void T_generalFieldaNotEnoughDataTest(final Map<Object, Object> dataMap, final List<Object> dataList) throws IOException{
    Path path = new Path( OUTPUT_FILE_PATH );
    IStreamWriter writer = new OrcStreamWriter( configuration , path , getGeneralField() );
    writer.write( dataMap );
    writer.write( dataList );
    writer.close();

    IStreamReader reader = new OrcStreamReader( configuration , path );
    while( reader.hasNext() ) {
      IParser parser = reader.next();
      assertEquals(VAL1, parser.get(COL1).getString());
      assertEquals(VAL2, parser.get(COL2).getInt()) ;
    }
  }


  @Test(dataProvider = "just_data")
  public void T_stringSchemaJustDataTest(final Map<Object, Object> dataMap, final List<Object> dataList) throws IOException{
    Path path = new Path( OUTPUT_FILE_PATH );
    IStreamWriter writer = new OrcStreamWriter( configuration , path , SCHEMA );
    writer.write( dataMap );
    writer.write( dataList );
    writer.close();

    IStreamReader reader = new OrcStreamReader( configuration , path );
    while( reader.hasNext() ) {
      IParser parser = reader.next();
      assertEquals( parser.get(COL1).getString(), VAL1 );
      assertEquals( parser.get(COL2).getInt() , VAL2 );
      assertEquals( parser.get(COL3).getLong() , VAL3 );
      testCol4(parser);
      testCol5(parser);
      testCol6(parser);
    }
  }

  @Test(dataProvider = "just_data")
  public void T_generalFieldJustDataTest(final Map<Object, Object> dataMap, final List<Object> dataList) throws IOException{
    Path path = new Path( OUTPUT_FILE_PATH );
    IStreamWriter writer = new OrcStreamWriter( configuration , path , getGeneralField() );
    writer.write( dataMap );
    writer.write( dataList );
    writer.close();

    IStreamReader reader = new OrcStreamReader( configuration , path );
    while( reader.hasNext() ) {
      IParser parser = reader.next();
      assertEquals( parser.get(COL1).getString(), VAL1 );
      assertEquals( parser.get(COL2).getInt() , VAL2 );
      assertEquals( parser.get(COL3).getLong() , VAL3 );
      testCol4(parser);
      testCol5(parser);
      testCol6(parser);
    }
  }

@Test(dataProvider = "too_long_data")
  public void T_stringSchemaTooLongDataTest(final Map<Object, Object> dataMap, final List<Object> dataList) throws IOException{
    Path path = new Path( OUTPUT_FILE_PATH );
    IStreamWriter writer = new OrcStreamWriter( configuration , path , SCHEMA );
    writer.write( dataMap );
    writer.write( dataList );
    writer.close();

    IStreamReader reader = new OrcStreamReader( configuration , path );
    while( reader.hasNext() ) {
      IParser parser = reader.next();
      assertEquals(VAL1, parser.get(COL1).getString());
      assertEquals(VAL2, parser.get(COL2).getInt()) ;
      assertEquals(VAL3, parser.get(COL3).getLong());
      testCol4(parser);
      testCol5(parser);
      testCol6(parser);
    }
  }

  @Test(dataProvider = "too_long_data")
  public void T_generalFieldTooLongDataTest(final Map<Object, Object> dataMap, final List<Object> dataList) throws IOException{
    Path path = new Path( OUTPUT_FILE_PATH );
    IStreamWriter writer = new OrcStreamWriter( configuration , path , getGeneralField() );
    writer.write( dataMap );
    writer.write( dataList );
    writer.close();

    IStreamReader reader = new OrcStreamReader( configuration , path );
    while( reader.hasNext() ) {
      IParser parser = reader.next();
      assertEquals(VAL1, parser.get(COL1).getString());
      assertEquals(VAL2, parser.get(COL2).getInt()) ;
      assertEquals(VAL3, parser.get(COL3).getLong());
      testCol4(parser);
      testCol5(parser);
      testCol6(parser);
    }
  }

}
