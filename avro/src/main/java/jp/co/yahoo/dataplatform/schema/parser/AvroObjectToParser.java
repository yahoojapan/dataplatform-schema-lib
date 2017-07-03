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

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.AvroTypeException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericArray;

public class AvroObjectToParser{

  private static final GenericData genericUtil = new GenericData();

  public static IParser get( final Object obj ) throws IOException{
    Schema.Type schemaType;
    try{
      schemaType = genericUtil.induce( obj ).getType();
    }catch( AvroTypeException e ){
      return new AvroNullParser();
    }

    switch( schemaType ){
      case ARRAY:
        return new AvroArrayParser( (GenericArray)obj );
      case MAP:
        return new AvroMapParser( (Map<Object,Object>)obj );
      case RECORD:
        return new AvroRecordParser( (GenericRecord)obj );
      case UNION :
      default:
        return new AvroNullParser();
    }
  }

  public static boolean hasParser( final Object obj ) throws IOException{
    Schema.Type schemaType;
    try{
      schemaType = genericUtil.induce( obj ).getType();
    }catch( AvroTypeException e ){
      return false;
    }

    switch( schemaType ){
      case ARRAY:
      case MAP:
      case RECORD:
        return true;
      case UNION :
      default:
        return false;
    }
  }

}
