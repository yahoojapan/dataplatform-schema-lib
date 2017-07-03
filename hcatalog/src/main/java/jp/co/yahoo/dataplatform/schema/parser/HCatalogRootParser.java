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
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;

public class HCatalogRootParser implements IParser {

  private final HCatRecord record;
  private final Map<String,Integer> fieldIndexMap;
  private final List<IHCatalogPrimitiveConverter> converterList;
  private final List<HCatFieldSchema> schemaList;

  public HCatalogRootParser( final HCatRecord record , final HCatSchema schema ) throws IOException{
    this.record = record;

    fieldIndexMap = new HashMap<String,Integer>();
    converterList = new ArrayList<IHCatalogPrimitiveConverter>();
    schemaList = new ArrayList<HCatFieldSchema>();

    for( int i = 0 ; i < schema.size() ; i++ ){
      HCatFieldSchema fieldSchema = schema.get(i);
      fieldIndexMap.put( fieldSchema.getName() , Integer.valueOf(i) );
      converterList.add( HCatalogPrimitiveConverterFactory.get( fieldSchema ) );
      schemaList.add( schema.get(i) );
    }
  }

  @Override
  public PrimitiveObject get(final String key ) throws IOException{
    if( ! containsKey( key ) ){
      return null;
    }
    return get( fieldIndexMap.get( key ) );
  }

  @Override
  public PrimitiveObject get( final int index ) throws IOException{
    return converterList.get( index ).get( record.get( index ) );
  }

  @Override
  public IParser getParser( final String key ) throws IOException{
    if( ! containsKey( key ) ){
      return new HCatalogNullParser();
    }
    return getParser( fieldIndexMap.get( key ) );
  }

  @Override
  public IParser getParser( final int index ) throws IOException{
    if( schemaList.size() <= index ){
      return new HCatalogNullParser();
    }
    return HCatalogParserFactory.get( schemaList.get( index ) , record.get( index ) );
  }

  @Override
  public String[] getAllKey() throws IOException{
    String[] keys = new String[schemaList.size()];
    for( int i = 0 ; i < schemaList.size() ; i++ ){
      keys[i] = schemaList.get(i).getName();
    }
    return keys;
  }

  @Override
  public boolean containsKey( final String key ) throws IOException{
    return fieldIndexMap.containsKey( key );
  }

  @Override
  public int size() throws IOException{
    return schemaList.size();
  }

  @Override
  public boolean isArray() throws IOException{
    return false;
  }

  @Override
  public boolean isMap() throws IOException{
    return false;
  }

  @Override
  public boolean isStruct() throws IOException{
    return true;
  }

  @Override
  public boolean hasParser( final int index ) throws IOException{
    if( schemaList.size() <= index ){
      return false;
    }
    return HCatalogParserFactory.hasParser( schemaList.get( index ) );
  }

  @Override
  public boolean hasParser( final String key ) throws IOException{
    if( ! containsKey( key ) ){
      return false;
    }
    return hasParser( fieldIndexMap.get( key ) );
  }

  @Override
  public Object toJavaObject() throws IOException{
    Map<String,Object> result = new HashMap<String,Object>();
    for( int i = 0 ; i < size() ; i++ ){
      String key = schemaList.get(i).getName();
      if( hasParser(key) ){
        result.put( key , getParser(key).toJavaObject() );
      }
      else{
        result.put( key , get(key) );
      }
    }

    return result;
  }
}
