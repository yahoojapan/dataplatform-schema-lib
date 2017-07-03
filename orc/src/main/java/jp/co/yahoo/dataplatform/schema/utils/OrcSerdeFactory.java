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
package jp.co.yahoo.dataplatform.schema.utils;

import java.io.IOException;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hive.ql.io.orc.OrcSerde;

import org.apache.hadoop.hive.serde.serdeConstants;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class OrcSerdeFactory {

  public static OrcSerde getFromString( final Configuration config , final String schemaString )throws IOException{
    return getFromTypeInfo( config , TypeInfoUtils.getTypeInfoFromTypeString( schemaString ) );
  }

  public static OrcSerde getFromObjectInspector( final Configuration config , final ObjectInspector ois ) throws IOException{
    return getFromTypeInfo( config , TypeInfoUtils.getTypeInfoFromObjectInspector( ois ) );
  }

  public static OrcSerde getFromTypeInfo( final Configuration config , final TypeInfo typeInfo )throws IOException{
    ObjectInspector objectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo( typeInfo );
    if( !( typeInfo instanceof StructTypeInfo ) ){
      throw new IOException( "Input type info is not StructTypeInfo : " + typeInfo.toString() );
    }
    String columnsName = "";
    String columnsType = "";
    List<TypeInfo> typeInfoList = ( (StructTypeInfo)typeInfo ).getAllStructFieldTypeInfos();
    List<StructField> structField = (List<StructField>)( ( (StructObjectInspector)objectInspector ).getAllStructFieldRefs() );
    for( int i = 0 ; i < structField.size() ; i++ ){
      if( ! columnsName.isEmpty() ){
        columnsName = columnsName.concat( "," );
        columnsType = columnsType.concat( "," );
      }
      columnsName = columnsName.concat( structField.get(i).getFieldName() );
      columnsType = columnsType.concat( typeInfoList.get(i).toString() );
    }

    OrcSerde serde = new OrcSerde();
    Properties table = new Properties();
    table.setProperty( serdeConstants.LIST_COLUMNS , columnsName );
    table.setProperty( serdeConstants.LIST_COLUMN_TYPES , columnsType );
    serde.initialize( config , table );

    return serde;
  }

  public static ObjectInspector getObjectInspector( final OrcSerde serde )throws IOException{
    try{
      return serde.getObjectInspector();
    }catch( SerDeException e ){
      throw new IOException( e );
    }
  }

}
