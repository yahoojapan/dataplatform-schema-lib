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

import jp.co.yahoo.dataplatform.schema.parser.HCatalogParserFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hive.hcatalog.mapreduce.*;
import org.apache.hive.hcatalog.data.*;
import org.apache.hive.hcatalog.data.schema.*;

import jp.co.yahoo.dataplatform.schema.parser.IParser;

public class HCatalogSampleMap extends Mapper<WritableComparable, HCatRecord, Text, LongWritable> {

  private final Text groupKey = new Text();
  private final LongWritable one = new LongWritable(1);
  private HCatSchema schema = null;

 @Override
 protected void setup( final Context context ) throws IOException , InterruptedException {
    schema = HCatInputFormat.getDataColumns( context.getConfiguration() );
  }

  @Override
  protected void map( WritableComparable key , HCatRecord val , Context context )throws IOException , InterruptedException{
    IParser parser = HCatalogParserFactory.get( schema , val );

    IParser adgeoParser = parser.getParser( "adgeo" );
    String adgeoRegion = null;
    if( adgeoParser.containsKey( "region" ) ){
      adgeoRegion = adgeoParser.get( "region" ).getString();
    }

    String spaceid = null;
    if( parser.containsKey( "spaceid" ) ){
      spaceid = parser.get( "spaceid" ).getString();
    }

    groupKey.set( String.format( "%s\t%s" , adgeoRegion , spaceid ) );

    context.write( groupKey , one );
  }
  
}
