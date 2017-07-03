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

import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;

public class HCatalogSchemaFactory{

  public static IField getGeneralSchema( final HCatSchema hCatSchema ) throws IOException{
    StructContainerField schema = new StructContainerField( "hcat_schema" );
    for( int i = 0 ; i < hCatSchema.size() ; i++ ){
      schema.set( getGeneralSchemaFromHCatFieldSchema( hCatSchema.get( i ) ) );
    }
    return schema;
  }

  public static IField getGeneralSchemaFromHCatFieldSchema( final HCatFieldSchema hCatFieldSchema ) throws IOException{
    if( hCatFieldSchema.getCategory() == HCatFieldSchema.Category.ARRAY ){
      HCatSchema arrayElementSchema = hCatFieldSchema.getArrayElementSchema();
      return new ArrayContainerField( hCatFieldSchema.getName() , getGeneralSchemaFromHCatFieldSchema( arrayElementSchema.get(0) ) );
    }
    else if( hCatFieldSchema.getCategory() == HCatFieldSchema.Category.MAP ){
      HCatSchema mapValueElementSchema = hCatFieldSchema.getMapValueSchema();
      return new MapContainerField( hCatFieldSchema.getName() , getGeneralSchemaFromHCatFieldSchema( mapValueElementSchema.get(0) ) );
    }
    else if( hCatFieldSchema.getCategory() == HCatFieldSchema.Category.STRUCT ){
      HCatSchema structSchema = hCatFieldSchema.getStructSubSchema();
      StructContainerField field = new StructContainerField( hCatFieldSchema.getName() );
      for( int i = 0 ; i < structSchema.size() ; i++ ){
        field.set( getGeneralSchemaFromHCatFieldSchema( structSchema.get(i) ) );
      }
      return field;
    }
    else if( hCatFieldSchema.getCategory() == HCatFieldSchema.Category.PRIMITIVE ){
      TypeInfo typeInfo = hCatFieldSchema.getTypeInfo();
      return HiveSchemaFactory.getGeneralSchema( hCatFieldSchema.getName() , typeInfo );
    }
    else{
      throw new IOException( "Unknown HCatalog field type : " + hCatFieldSchema.toString() );
    }
  }

  public static HCatSchema getHCatSchema( final IField schema ) throws IOException{
    if( !( schema instanceof StructContainerField ) ){
      throw new IOException( "Root schema is struct only." );
    }
    StructContainerField structSchema = (StructContainerField)schema;
    
    List<HCatFieldSchema> hCatSchemaList = new ArrayList<HCatFieldSchema>();
    for( String key : structSchema.getKeys() ){
      hCatSchemaList.add( getHCatFieldSchema( structSchema.get( key ) ) );
    }

    return new HCatSchema( hCatSchemaList );
  }

  public static HCatFieldSchema getHCatFieldSchema( final IField schema ) throws IOException{
    if( schema instanceof ArrayContainerField  ){
      ArrayContainerField arraySchema = (ArrayContainerField)schema;
      List<HCatFieldSchema> hCatSchemaList = new ArrayList<HCatFieldSchema>();
      hCatSchemaList.add( getHCatFieldSchema( arraySchema.getField() ) );
      return new HCatFieldSchema( schema.getName() , HCatFieldSchema.Type.ARRAY , new HCatSchema( hCatSchemaList ) , schema.getProperties().toString() );
    }
    else if( schema instanceof MapContainerField ){
      MapContainerField mapSchema = (MapContainerField)schema;
      PrimitiveTypeInfo keyTypeInfo = TypeInfoFactory.getPrimitiveTypeInfo( TypeInfoFactory.stringTypeInfo.getTypeName() );
      List<HCatFieldSchema> hCatSchemaList = new ArrayList<HCatFieldSchema>();
      hCatSchemaList.add( getHCatFieldSchema( mapSchema.getField() ) );
      return HCatFieldSchema.createMapTypeFieldSchema( schema.getName() , keyTypeInfo , new HCatSchema( hCatSchemaList ) , schema.getProperties().toString() );
    }
    else if( schema instanceof StructContainerField ){
      StructContainerField structSchema = (StructContainerField)schema;
      List<HCatFieldSchema> hCatSchemaList = new ArrayList<HCatFieldSchema>();
      for( String key : structSchema.getKeys() ){
        hCatSchemaList.add( getHCatFieldSchema( structSchema.get( key ) ) );
      }
      return new HCatFieldSchema( schema.getName() , HCatFieldSchema.Type.STRUCT , new HCatSchema( hCatSchemaList ) , schema.getProperties().toString() );
    }
    else{
      TypeInfo typeInfo = HiveSchemaFactory.getHiveSchema( schema );
      if( ! ( typeInfo instanceof PrimitiveTypeInfo ) ){
        throw new IOException( "Unknown schema type : " + typeInfo.toString() );
      }
      return new HCatFieldSchema( schema.getName() , (PrimitiveTypeInfo)typeInfo , schema.getProperties().toString() );
    }
  }

}
