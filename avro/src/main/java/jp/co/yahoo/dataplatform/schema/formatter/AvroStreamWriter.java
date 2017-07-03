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
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.List;
import java.util.Map;

import jp.co.yahoo.dataplatform.schema.design.AvroSchemaFactory;
import jp.co.yahoo.dataplatform.schema.design.IField;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;
import jp.co.yahoo.dataplatform.schema.parser.IParser;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;

public class AvroStreamWriter implements IStreamWriter{

  private final DataFileWriter<Object> writer;
  private final Schema avroSchema;
  private final IAvroFormatter avroFormatter;

  public AvroStreamWriter( final OutputStream out , final File avroSchemaFile ) throws IOException{
    avroSchema = new Schema.Parser().parse( avroSchemaFile );
    DatumWriter<Object> datumWriter = new GenericDatumWriter<Object>( avroSchema );
    writer = new DataFileWriter<Object>( datumWriter );
    writer.create( avroSchema , out );
    avroFormatter = AvroFormatterFactory.get( avroSchema );
  }

  public AvroStreamWriter( final OutputStream out , final String schemaString ) throws IOException{
    avroSchema = new Schema.Parser().parse( schemaString );
    DatumWriter<Object> datumWriter = new GenericDatumWriter<Object>( avroSchema );
    writer = new DataFileWriter<Object>( datumWriter );
    writer.create( avroSchema , out );
    avroFormatter = AvroFormatterFactory.get( avroSchema );
  }

  public AvroStreamWriter( final OutputStream out , final InputStream schemaInputStream ) throws IOException{
    avroSchema = new Schema.Parser().parse( schemaInputStream );
    DatumWriter<Object> datumWriter = new GenericDatumWriter<Object>( avroSchema );
    writer = new DataFileWriter<Object>( datumWriter );
    writer.create( avroSchema , out );
    avroFormatter = AvroFormatterFactory.get( avroSchema );
  }

  public AvroStreamWriter( final OutputStream out , final Schema avroSchema ) throws IOException{
    this.avroSchema = avroSchema;
    DatumWriter<Object> datumWriter = new GenericDatumWriter<Object>( avroSchema );
    writer = new DataFileWriter<Object>( datumWriter );
    writer.create( avroSchema , out );
    avroFormatter = AvroFormatterFactory.get( avroSchema );
  }

  public AvroStreamWriter( final OutputStream out , final IField schema ) throws IOException{
    this.avroSchema = AvroSchemaFactory.getAvroSchema( schema );
    DatumWriter<Object> datumWriter = new GenericDatumWriter<Object>( avroSchema );
    writer = new DataFileWriter<Object>( datumWriter );
    writer.create( avroSchema , out );
    avroFormatter = AvroFormatterFactory.get( avroSchema );
  }

  @Override
  public void write( final PrimitiveObject obj ) throws IOException{
    avroFormatter.clear();
    Object writeObj = avroFormatter.write( obj );
    writer.append( writeObj );
  }

  @Override
  public void write( final List<Object> array ) throws IOException{
    avroFormatter.clear();
    Object writeObj = avroFormatter.write( array );
    writer.append( writeObj );
  }

  @Override
  public void write( final Map<Object,Object> map ) throws IOException{
    avroFormatter.clear();
    Object writeObj = avroFormatter.write( map );
    writer.append( writeObj );
  }

  @Override
  public void write( final IParser parser ) throws IOException{
    avroFormatter.clear();
    Object writeObj = avroFormatter.writeParser( null , parser );
    writer.append( writeObj );
  }

  @Override
  public void close() throws IOException{
    writer.close();
  }

}
