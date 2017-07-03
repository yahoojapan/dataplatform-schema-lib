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

import java.util.List;
import java.util.Map;

import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;

public class HCatalogParserFactory{

  public static IParser get( final HCatSchema hcatSchema , final HCatRecord hcatRecord ) throws IOException{
    return new HCatalogRootParser( hcatRecord , hcatSchema );
  }

  public static IParser get( final HCatFieldSchema hCatFieldSchema , final Object data ) throws IOException{

    switch( hCatFieldSchema.getCategory() ){
      case ARRAY:
        if( data instanceof List ){
          return new HCatalogStructParser( (List<Object>)data , hCatFieldSchema.getArrayElementSchema() );
        }
      case MAP:
        if( data instanceof Map ){
          return new HCatalogMapParser( (Map<Object,Object>)data , hCatFieldSchema.getMapValueSchema() );
        }
      case STRUCT:
        if( data instanceof List ){
          return new HCatalogStructParser( (List<Object>)data , hCatFieldSchema.getStructSubSchema() );
        }
    }
    return new HCatalogNullParser();
  }

  public static boolean hasParser( final HCatFieldSchema hCatFieldSchema ) throws IOException{
    switch( hCatFieldSchema.getCategory() ){
      case ARRAY:
      case MAP:
      case STRUCT:
        return true;
      default:
        return false;
    }
  }

}
