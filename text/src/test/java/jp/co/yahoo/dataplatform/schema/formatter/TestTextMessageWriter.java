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

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import static org.testng.Assert.assertEquals;

import jp.co.yahoo.dataplatform.schema.utils.Properties;
import org.testng.annotations.Test;

import jp.co.yahoo.dataplatform.schema.design.LongField;
import jp.co.yahoo.dataplatform.schema.design.StringField;
import jp.co.yahoo.dataplatform.schema.design.ArrayContainerField;
import jp.co.yahoo.dataplatform.schema.design.MapContainerField;
import jp.co.yahoo.dataplatform.schema.design.StructContainerField;

public class TestTextMessageWriter {

  @Test
  public void T_example_array() throws IOException{
    Properties properties = new Properties();
    properties.set( "delimiter" , "0x2c" );
    ArrayContainerField schema = new ArrayContainerField( "array" , new LongField( "array_value" ) , properties );

    IMessageWriter writer = new TextMessageWriter( schema );

    List<Object> objList = new ArrayList<Object>();
    objList.add( (long)100000 );
    objList.add( (double)2.4 );
    objList.add( (short)1 );
    objList.add( (float)10.3 );
    objList.add( 100 );

    byte[] out = writer.create( objList );
    System.out.println( new String( out , "UTF-8") );
  }


  @Test
  public void T_example_map() throws IOException{
    Properties properties = new Properties();
    properties.set( "delimiter" , "0x20" );
    properties.set( "field_delimiter" , "0x2c" );
    MapContainerField schema = new MapContainerField( "map" , new LongField( "map_value" ) , properties );

    IMessageWriter writer = new TextMessageWriter( schema );

    Map<Object,Object> objList = new HashMap<Object,Object>();
    objList.put( "f1" , 100000 );
    objList.put( "f2" , 100000 );
    objList.put( "f3" , 0 );
    objList.put( "f4" , -100 );

    byte[] out = writer.create( objList );
    System.out.println( new String( out , "UTF-8") );
  }

  @Test
  public void T_example_struct() throws IOException{
    Properties properties = new Properties();
    properties.set( "delimiter" , "0x20" );
    StructContainerField schema = new StructContainerField( "struct" , properties );
    schema.set( new StringField( "f1" ) );
    schema.set( new StringField( "f2" ) );
    schema.set( new StringField( "f3" ) );

    IMessageWriter writer = new TextMessageWriter( schema );

    Map<Object,Object> objList = new HashMap<Object,Object>();
    objList.put( "f1" , "abc" );
    objList.put( "f2" , "defg" );
    objList.put( "f3" , "hijkl" );
    objList.put( "f4" , "mnopqr" );

    byte[] out = writer.create( objList );
    System.out.println( new String( out , "UTF-8") );
  }

}
