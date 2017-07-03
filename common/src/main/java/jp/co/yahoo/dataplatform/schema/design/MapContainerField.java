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
import java.util.HashMap;

import jp.co.yahoo.dataplatform.schema.utils.Properties;

public class MapContainerField implements INamedContainerField {

  private final String name;
  private final IField defaultField;
  private final Properties properties;
  private final Map<String,IField> fieldContainer = new HashMap<String,IField>();

  public MapContainerField( final String name , final IField defaultField ){
    this.name = name;
    this.defaultField = defaultField;
    properties = new Properties();
  }

  public MapContainerField( final String name , final IField defaultField ,  final Properties properties ){
    this.name = name;
    this.defaultField = defaultField;
    this.properties = properties;
  }

  @Override
  public String getName(){
    return name;
  }

  @Override
  public IField getField(){
    return defaultField;
  }

  @Override
  public void set( final IField field ) throws IOException{
    String fieldName = field.getName();
    fieldContainer.put( fieldName , field );
  }

  @Override
  public IField get( final String key ) throws IOException{
    if( containsKey( key ) ){
      return fieldContainer.get( key );
    }
    else{
      return getField();
    }
  }

  @Override
  public boolean containsKey( final String key ) throws IOException{
    return fieldContainer.containsKey( key );
  }

  @Override
  public String[] getKeys() throws IOException{
    String[] keyArray = new String[ fieldContainer.size() ];

    int i = 0;
    for( Map.Entry<String,IField> entry : fieldContainer.entrySet() ){
      keyArray[i] = entry.getKey();
      i++;
    }
    return keyArray;
  }

  @Override
  public Properties getProperties(){
    return properties;
  }

  @Override
  public FieldType getFieldType(){
    return FieldType.MAP;
  }

}
