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
import java.util.HashMap;
import java.util.Iterator;

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;

public class HiveMapParser implements IHiveParser {

  private final MapObjectInspector mapObjectInspector;
  private final ObjectInspector childObjectInspector;
  private final IHivePrimitiveConverter childConverter;
  private final boolean childHasParser;
  private Object row;

  public HiveMapParser( final MapObjectInspector mapObjectInspector ){
    this.mapObjectInspector = mapObjectInspector;
    childObjectInspector = mapObjectInspector.getMapValueObjectInspector();
    childConverter = HivePrimitiveConverterFactory.get( childObjectInspector );

    childHasParser = HiveParserFactory.hasParser( childObjectInspector );
  }

  @Override
  public void setObject( final Object row ) throws IOException{
    this.row = row;
  }

  @Override
  public PrimitiveObject get(final String key ) throws IOException{
    return childConverter.get( mapObjectInspector.getMapValueElement( row , new Text( key ) ) );
  }

  @Override
  public PrimitiveObject get( final int index ) throws IOException{
    return get( Integer.toString( index ) );
  }

  @Override
  public IParser getParser( final String key ) throws IOException{
    IHiveParser childParser =  HiveParserFactory.get( childObjectInspector );
    childParser.setObject( mapObjectInspector.getMapValueElement( row , new Text( key ) ) );
    return childParser;
  }

  @Override
  public IParser getParser( final int index ) throws IOException{
    return getParser( Integer.toString( index ) );
  }

  @Override
  public String[] getAllKey() throws IOException{
    Map<?,?> map = mapObjectInspector.getMap( row );

    String[] keys = new String[map.size()];

    Iterator<?> keyIterator = map.keySet().iterator();
    int i = 0;
    while( keyIterator.hasNext() ){
      keys[i] = keyIterator.next().toString();
      i++;
    }

    return keys;
  }

  @Override
  public boolean containsKey( final String key ) throws IOException{
    return get( key ) != null;
  }

  @Override
  public int size() throws IOException{
    return mapObjectInspector.getMapSize( row );
  }

  @Override
  public boolean isArray() throws IOException{
    return false;
  }

  @Override
  public boolean isMap() throws IOException{
    return true;
  }

  @Override
  public boolean isStruct() throws IOException{
    return false;
  }

  @Override
  public boolean hasParser( final int index ) throws IOException{
    return childHasParser;
  }

  @Override
  public boolean hasParser( final String key ) throws IOException{
    return childHasParser;
  }

  @Override
  public Object toJavaObject() throws IOException{
    Map<String,Object> result = new HashMap<String,Object>();
    for( String key : getAllKey() ){
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
