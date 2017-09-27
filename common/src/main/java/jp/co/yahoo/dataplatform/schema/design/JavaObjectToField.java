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
package jp.co.yahoo.dataplatform.schema.design;

import java.io.IOException;

import java.util.Map;
import java.util.List;

import jp.co.yahoo.dataplatform.schema.utils.Properties;

public final class JavaObjectToField{

  private JavaObjectToField(){}

  public static IField get( final Map<Object,Object> javaObject ) throws IOException{
    String type = javaObject.get( "type" ).toString();
    Properties properties = new Properties();
    Map<Object,Object> propertiesObj = (Map<Object,Object>)javaObject.get( "properties" );
    for( Map.Entry<Object,Object> entry : propertiesObj.entrySet() ){
      properties.set( entry.getKey().toString() , entry.getValue().toString() );
    }
    String name = javaObject.get( "name" ).toString();
    List<Map<Object,Object>> child = (List<Map<Object,Object>>)javaObject.get( "child" );

    switch( type ){
      case "ARRAY":
        IField arrayChild = get( child.get(0) );
        return new ArrayContainerField( name , arrayChild , properties );
      case "UNION":
        UnionField union = new UnionField( name , properties );
        for( Map<Object,Object> childObj : child ){
          union.set( get( childObj ) );
        }
        return union;
      case "MAP":
        IField defaultField = get( (Map<Object,Object>)javaObject.get( "default" ) );
        MapContainerField map = new MapContainerField( name , defaultField , properties );
        for( Map<Object,Object> childObj : child ){
          map.set( get( childObj ) );
        }
        return map;
      case "STRUCT":
        StructContainerField struct = new StructContainerField( name , properties );
        for( Map<Object,Object> childObj : child ){
          struct.set( get( childObj ) );
        }
        return struct;

      case "BOOLEN":
        return new BooleanField( name , properties );
      case "BYTE":
        return new ByteField( name , properties );
      case "BYTES":
        return new BytesField( name , properties );
      case "DOUBLE":
        return new DoubleField( name , properties );
      case "FLOAT":
        return new FloatField( name , properties );
      case "INTEGER":
        return new IntegerField( name , properties );
      case "LONG":
        return new LongField( name , properties );
      case "SHORT":
        return new ShortField( name , properties );
      case "STRING":
        return new StringField( name , properties );
      case "NULL":
        return new NullField( name , properties );
      default:
        throw new IOException( "Invalid schema type." );
    }
  }

}
