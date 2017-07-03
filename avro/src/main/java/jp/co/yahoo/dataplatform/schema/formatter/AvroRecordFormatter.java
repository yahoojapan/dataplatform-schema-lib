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

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;
import jp.co.yahoo.dataplatform.schema.parser.IParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class AvroRecordFormatter implements IAvroFormatter{

  private final Schema avroSchema;
  private final List<KeyAndFormatter> childContainer;

  public AvroRecordFormatter( final Schema avroSchema ){
    this.avroSchema = avroSchema;

    childContainer = new ArrayList<KeyAndFormatter>();
    List<Schema.Field> childFields = avroSchema.getFields();
    for( Schema.Field field : childFields ){
      childContainer.add( new KeyAndFormatter( field.name() , field.schema() ) );
    }
  }

  @Override
  public Object write( final Object obj ) throws IOException{
    GenericRecord record = new GenericData.Record( avroSchema );
    if( ! ( obj instanceof Map ) ){
      return record;
    }

    Map<Object,Object> mapObj = (Map<Object,Object>)obj;

    for( KeyAndFormatter childFormatter : childContainer ){
      childFormatter.clear();
      record.put( childFormatter.getName() , childFormatter.get( mapObj ) );
    }

    return record;
  }

  @Override
  public Object writeParser(final PrimitiveObject obj , final IParser parser ) throws IOException{
    GenericRecord record = new GenericData.Record( avroSchema );

    for( KeyAndFormatter childFormatter : childContainer ){
      childFormatter.clear();
      record.put( childFormatter.getName() , childFormatter.get( parser ) );
    }

    return record;
  }

  @Override
  public void clear() throws IOException{
    for( KeyAndFormatter childFormatter : childContainer ){
      childFormatter.clear();
    }
  }

  private class KeyAndFormatter{

    private final String key;
    private final IAvroFormatter formatter;

    public KeyAndFormatter( final String key , final Schema avroSchema ){
      this.key = key;
      formatter = AvroFormatterFactory.get( avroSchema );
    }

    public String getName(){
      return key;
    }

    public Object get( final Map<Object,Object> map ) throws IOException{
      return formatter.write( map.get( key ) );
    }

    public Object get( final IParser parser ) throws IOException{
      return formatter.writeParser( parser.get( key ) , parser.getParser( key ) );
    }

    public void clear() throws IOException{
      formatter.clear();
    }
  }

}
