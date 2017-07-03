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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import jp.co.yahoo.dataplatform.schema.design.IField;
import jp.co.yahoo.dataplatform.schema.design.HiveSchemaFactory;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;
import jp.co.yahoo.dataplatform.schema.parser.IParser;

public class OrcStreamWriter implements IStreamWriter{

  private final Writer writer;
  private final IOrcFormatter formatter;

  public OrcStreamWriter( final Configuration config, final Path path, final String schema ) throws IOException{
    FileSystem fs = FileSystem.get(config);
    long stripeSize = HiveConf.getLongVar(config, HiveConf.ConfVars.HIVE_ORC_DEFAULT_STRIPE_SIZE);
    CompressionKind compress = CompressionKind.valueOf(HiveConf.getVar(config, HiveConf.ConfVars.HIVE_ORC_DEFAULT_COMPRESS));
    int bufferSize = HiveConf.getIntVar(config, HiveConf.ConfVars.HIVE_ORC_DEFAULT_BUFFER_SIZE);
    int rowIndexStride =  HiveConf.getIntVar(config, HiveConf.ConfVars.HIVE_ORC_DEFAULT_ROW_INDEX_STRIDE);

    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString( schema );
    ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo( typeInfo );
    writer = OrcFile.createWriter( fs, path, config, inspector, stripeSize, compress, bufferSize, rowIndexStride );
    formatter = OrcFormatterFactory.get( typeInfo );
  }

  public OrcStreamWriter( final Configuration config, final Path path, final TypeInfo typeInfo ) throws IOException{
    FileSystem fs = FileSystem.get(config);
    long stripeSize = HiveConf.getLongVar(config, HiveConf.ConfVars.HIVE_ORC_DEFAULT_STRIPE_SIZE);
    CompressionKind compress = CompressionKind.valueOf(HiveConf.getVar(config, HiveConf.ConfVars.HIVE_ORC_DEFAULT_COMPRESS));
    int bufferSize = HiveConf.getIntVar(config, HiveConf.ConfVars.HIVE_ORC_DEFAULT_BUFFER_SIZE);
    int rowIndexStride =  HiveConf.getIntVar(config, HiveConf.ConfVars.HIVE_ORC_DEFAULT_ROW_INDEX_STRIDE);

    ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo( typeInfo );
    writer = OrcFile.createWriter( fs, path, config, inspector, stripeSize, compress, bufferSize, rowIndexStride );
    formatter = OrcFormatterFactory.get( typeInfo );
  }

  public OrcStreamWriter( final Configuration config, final Path path, final IField schema ) throws IOException{
    FileSystem fs = FileSystem.get(config);
    long stripeSize = HiveConf.getLongVar(config, HiveConf.ConfVars.HIVE_ORC_DEFAULT_STRIPE_SIZE);
    CompressionKind compress = CompressionKind.valueOf(HiveConf.getVar(config, HiveConf.ConfVars.HIVE_ORC_DEFAULT_COMPRESS));
    int bufferSize = HiveConf.getIntVar(config, HiveConf.ConfVars.HIVE_ORC_DEFAULT_BUFFER_SIZE);
    int rowIndexStride =  HiveConf.getIntVar(config, HiveConf.ConfVars.HIVE_ORC_DEFAULT_ROW_INDEX_STRIDE);
    TypeInfo typeInfo = HiveSchemaFactory.getHiveSchema( schema ); 

    ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo( typeInfo );

    writer = OrcFile.createWriter( fs, path, config, inspector, stripeSize, compress, bufferSize, rowIndexStride );
    formatter = OrcFormatterFactory.get( typeInfo );
  }

  @Override
  public void write( final PrimitiveObject obj ) throws IOException{
    writer.addRow( formatter.write( obj ) );
  }

  @Override
  public void write( final List<Object> array ) throws IOException{
    writer.addRow( formatter.write( array ) );
  }

  @Override
  public void write( final Map<Object,Object> map ) throws IOException{
    writer.addRow( formatter.write( map ) );
  }

  @Override
  public void write( final IParser parser ) throws IOException{
    writer.addRow( formatter.writeFromParser( null , parser ) );
  }

  @Override
  public void close() throws IOException{
    writer.close();
  }

}
