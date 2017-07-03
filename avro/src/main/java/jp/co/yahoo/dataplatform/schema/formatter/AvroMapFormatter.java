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

import java.util.Map;
import java.util.HashMap;

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;
import jp.co.yahoo.dataplatform.schema.parser.IParser;
import org.apache.avro.Schema;

public class AvroMapFormatter implements IAvroFormatter{

  private final Schema childSchema;
  private final Map<Object,Object> map;

  public AvroMapFormatter( final Schema avroSchema ){
    childSchema = avroSchema.getValueType();
    map = new HashMap<Object,Object>();
  }

  @Override
  public Object write( final Object obj ) throws IOException{
    map.clear();
    if( ! ( obj instanceof Map ) ){
      return map;
    }

    Map<Object,Object> mapObj = (Map<Object,Object>)obj;

    for( Map.Entry<Object,Object> entry : mapObj.entrySet() ){
      IAvroFormatter formatter = AvroFormatterFactory.get( childSchema );
      map.put( entry.getKey() ,  formatter.write( entry.getValue() ) );
    }

    return map;
  }

  @Override
  public Object writeParser(final PrimitiveObject obj , final IParser parser ) throws IOException{
    map.clear();
    for( String key : parser.getAllKey() ){
      IAvroFormatter formatter = AvroFormatterFactory.get( childSchema );
      map.put( key ,  formatter.writeParser( parser.get( key ) , parser.getParser( key ) ) );
    }

    return map;
  }

  @Override
  public void clear() throws IOException{
    map.clear();
  }

}
