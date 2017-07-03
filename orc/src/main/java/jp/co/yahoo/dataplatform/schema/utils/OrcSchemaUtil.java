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

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class OrcSchemaUtil {

  public static String getOrcSchemaString( final Configuration config , final String target ) throws IOException{
    return getOrcTypeInfo( config , target ).toString();
  }

  public static String getOrcSchemaString( final Configuration config , final Path path ) throws IOException{
    return getOrcTypeInfo( config , path ).toString();
  }

  public static TypeInfo getOrcTypeInfo( final Configuration config , final String target ) throws IOException{
    return TypeInfoUtils.getTypeInfoFromObjectInspector( getOrcObjectInspector( config , target ) );
  }

  public static TypeInfo getOrcTypeInfo( final Configuration config , final Path path ) throws IOException{
    return TypeInfoUtils.getTypeInfoFromObjectInspector( getOrcObjectInspector( config , path ) );
  }

  public static ObjectInspector getOrcObjectInspector( final Configuration config , final String target ) throws IOException{
    return getOrcObjectInspector( config ,  new Path( target ) );

  }

  public static ObjectInspector getOrcObjectInspector( final Configuration config , final Path path ) throws IOException{
    Random rnd = new Random();

    FileSystem fs = FileSystem.get( config );
    FileStatus[] fsStatus = fs.listStatus( path );
    Path sampleOrcPath = fsStatus[rnd.nextInt( fsStatus.length )].getPath();
    Reader reader = OrcFile.createReader( sampleOrcPath , OrcFile.readerOptions( config ) );
    return reader.getObjectInspector();
  }


}
