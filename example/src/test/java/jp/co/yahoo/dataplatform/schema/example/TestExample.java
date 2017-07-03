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
package jp.co.yahoo.dataplatform.schema.example;

import java.io.IOException;
import java.io.File;

import java.util.Map;
import java.util.HashMap;

import static org.testng.Assert.assertEquals;

import jp.co.yahoo.dataplatform.schema.design.AvroSchemaConverter;
import jp.co.yahoo.dataplatform.schema.design.IField;
import jp.co.yahoo.dataplatform.schema.formatter.AvroMessageWriter;
import jp.co.yahoo.dataplatform.schema.formatter.IMessageWriter;
import jp.co.yahoo.dataplatform.schema.formatter.IStreamWriter;
import jp.co.yahoo.dataplatform.schema.formatter.OrcStreamWriter;
import org.testng.annotations.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import jp.co.yahoo.dataplatform.schema.parser.IParser;
import jp.co.yahoo.dataplatform.schema.parser.IMessageReader;
import jp.co.yahoo.dataplatform.schema.parser.IStreamReader;
import jp.co.yahoo.dataplatform.schema.parser.AvroMessageReader;
import jp.co.yahoo.dataplatform.schema.parser.OrcStreamReader;

public class TestExample{

  @Test
  public void T_avro_to_orc() throws IOException{
    String fileName = this.getClass().getClassLoader().getResource( "schema/avro/example_1.avsc" ).getPath();
    IMessageWriter writer = new AvroMessageWriter( new File( fileName ) );
    Map<Object,Object> mapContainer = new HashMap<Object,Object>();
    mapContainer.put( "ip" , "100" );
    mapContainer.put( "timestamp" , 200 );
    mapContainer.put( "url" , "300" );
    mapContainer.put( "referrer" , "400" );
    mapContainer.put( "useragent" , "500" );
    mapContainer.put( "sessionid" , 600 );
    mapContainer.put( "abcd_long" , 120304 );

    byte[] avroBytes = writer.create( mapContainer );

    IMessageReader reader = new AvroMessageReader( new File( fileName ) );
    IParser parser = reader.create( avroBytes );

    IField schema = new AvroSchemaConverter( new File( fileName ) ).get();

    String dir = "/tmp";
    String writeFileName = dir + "/bbb.orc";

    File file = new File( writeFileName );
    if( file.exists() ){
      file.delete();
    }

    Configuration config = new Configuration();
    config.set( "fs.default.name" , "file:///" );
    Path path = new Path( writeFileName );

    try{
      IStreamWriter orcWriter = new OrcStreamWriter( config , path , schema );
      orcWriter.write( parser );
      orcWriter.close();

      IStreamReader orcReader = new OrcStreamReader( config , path );
      while( orcReader.hasNext() ){
        IParser orcParser = orcReader.next();
        System.out.println( orcParser.get( "ip" ).getString() );
        System.out.println( orcParser.get( "timestamp" ).getString() );
        System.out.println( orcParser.get( "url" ).getString() );
        System.out.println( orcParser.get( "referrer" ).getString() );
        System.out.println( orcParser.get( "useragent" ).getString() );
        System.out.println( orcParser.get( "sessionid" ).getString() );
        System.out.println( orcParser.get( "sessionid2" ) );
        System.out.println( orcParser.get( "sessionid3" ).getString() );
        System.out.println( orcParser.get( "abcd_long" ).getString() );
      }
    }finally{
      file.delete();
    }

  }

}
