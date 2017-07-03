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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

public class HiveSchemaFactory{

  public static IField getGeneralSchema( final String fieldName , final TypeInfo typeInfo ) throws IOException{
    if( typeInfo.getCategory()  == ObjectInspector.Category.LIST ){
      ListTypeInfo listTypeInfo = (ListTypeInfo)typeInfo;
      return new ArrayContainerField( fieldName , getGeneralSchema( "array_value" , listTypeInfo.getListElementTypeInfo() ) );
    }
    else if( typeInfo.getCategory()  == ObjectInspector.Category.MAP ){
      MapTypeInfo mapTypeInfo = (MapTypeInfo)typeInfo;
      return new MapContainerField( fieldName , getGeneralSchema( "map_value" , mapTypeInfo.getMapValueTypeInfo() ) );
    }
    else if( typeInfo.getCategory()  == ObjectInspector.Category.STRUCT ){
      StructTypeInfo structTypeInfo = (StructTypeInfo)typeInfo;
      StructContainerField field = new StructContainerField( fieldName );
      
      List<TypeInfo> childTypeInfoList = structTypeInfo.getAllStructFieldTypeInfos();
      List<String> childFieldNameList = structTypeInfo.getAllStructFieldNames();
      for( int i = 0 ; i < childFieldNameList.size() ; i++ ){
        field.set( getGeneralSchema( childFieldNameList.get(i) , childTypeInfoList.get(i) ) );
      }
      return field;
    }
    else if( typeInfo.getCategory()  == ObjectInspector.Category.UNION ){
      UnionTypeInfo unionTypeInfo = (UnionTypeInfo)typeInfo;
      UnionField field = new UnionField( fieldName );
      
      for( TypeInfo childTypeInfo : unionTypeInfo.getAllUnionObjectTypeInfos() ){
        field.set( getGeneralSchema( childTypeInfo.getCategory().toString() , childTypeInfo ) );
      }
      return field;
    }
    else if( typeInfo.getCategory()  == ObjectInspector.Category.PRIMITIVE ){
      PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo)typeInfo;
      if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.BINARY ){
        return new BytesField( fieldName );
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN ){
        return new BooleanField( fieldName );
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.BYTE ){
        return new ByteField( fieldName );
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.DOUBLE ){
        return new DoubleField( fieldName );
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.FLOAT ){
        return new FloatField( fieldName );
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT ){
        return new IntegerField( fieldName );
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.LONG ){
        return new LongField( fieldName );
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.SHORT ){
        return new ShortField( fieldName );
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING ){
        return new StringField( fieldName );
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP ){
        return new NullField( fieldName );
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.VOID ){
        return new NullField( fieldName );
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.UNKNOWN ){
        return new NullField( fieldName );
      }
      else {
        return new NullField( fieldName );
      }
    }
    else{
      return null;
    }

  }

  public static IField getGeneralSchema( final String fieldName , final ObjectInspector hiveSchema ) throws IOException{
    return getGeneralSchema( fieldName , TypeInfoUtils.getTypeInfoFromObjectInspector( hiveSchema ) );
  }

  public static TypeInfo getHiveSchema( final IField schema ) throws IOException{
    if( schema instanceof ArrayContainerField ){
      return new HiveArraySchema( (ArrayContainerField)schema ).getHiveSchema();
    }
    else if( schema instanceof MapContainerField ){
      return new HiveMapSchema( (MapContainerField)schema ).getHiveSchema();
    }
    else if( schema instanceof StructContainerField ){
      return new HiveStructSchema( (StructContainerField)schema ).getHiveSchema();
    }
    else if( schema instanceof UnionField ){
      return new HiveUnionSchema( (UnionField)schema ).getHiveSchema();
    }
    else if( schema instanceof BooleanField ){
      return TypeInfoFactory.getPrimitiveTypeInfo( TypeInfoFactory.booleanTypeInfo.getTypeName() );
    }
    else if( schema instanceof ByteField ){
      return TypeInfoFactory.getPrimitiveTypeInfo( TypeInfoFactory.byteTypeInfo.getTypeName() );
    }
    else if( schema instanceof BytesField ){
      return TypeInfoFactory.getPrimitiveTypeInfo( TypeInfoFactory.binaryTypeInfo.getTypeName() );
    }
    else if( schema instanceof DoubleField ){
      return TypeInfoFactory.getPrimitiveTypeInfo( TypeInfoFactory.doubleTypeInfo.getTypeName() );
    }
    else if( schema instanceof FloatField ){
      return TypeInfoFactory.getPrimitiveTypeInfo( TypeInfoFactory.floatTypeInfo.getTypeName() );
    }
    else if( schema instanceof IntegerField ){
      return TypeInfoFactory.getPrimitiveTypeInfo( TypeInfoFactory.intTypeInfo.getTypeName() );
    }
    else if( schema instanceof LongField ){
      return TypeInfoFactory.getPrimitiveTypeInfo( TypeInfoFactory.longTypeInfo.getTypeName() );
    }
    else if( schema instanceof ShortField ){
      return TypeInfoFactory.getPrimitiveTypeInfo( TypeInfoFactory.shortTypeInfo.getTypeName() );
    }
    else if( schema instanceof StringField ){
      return TypeInfoFactory.getPrimitiveTypeInfo( TypeInfoFactory.stringTypeInfo.getTypeName() );
    }
    else if( schema instanceof NullField ){
      return TypeInfoFactory.getPrimitiveTypeInfo( TypeInfoFactory.stringTypeInfo.getTypeName() );
    }
    else {
      //void , null , unknown だと変換できないエラーとなるため、string に統一
      //return TypeInfoFactory.getPrimitiveTypeInfo( TypeInfoFactory.unknownTypeInfo.getTypeName() );
      return TypeInfoFactory.getPrimitiveTypeInfo( TypeInfoFactory.stringTypeInfo.getTypeName() );
    }
  }

}
