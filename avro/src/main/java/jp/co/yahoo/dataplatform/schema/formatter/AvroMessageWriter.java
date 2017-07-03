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
import java.io.ByteArrayOutputStream;

import java.util.List;
import java.util.Map;

import jp.co.yahoo.dataplatform.schema.design.AvroSchemaFactory;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.generic.GenericDatumWriter;

import jp.co.yahoo.dataplatform.schema.design.IField;
import jp.co.yahoo.dataplatform.schema.parser.IParser;

public class AvroMessageWriter implements IMessageWriter {

  private final ByteArrayOutputStream out;
  private final Encoder encoder;
  private final DatumWriter<Object> writer;
  private final IAvroFormatter avroFormatter;

  public AvroMessageWriter( final File avroSchemaFile ) throws IOException{
    Schema avroSchema = new Schema.Parser().parse( avroSchemaFile );
    writer = new GenericDatumWriter<Object>( avroSchema );
    out = new ByteArrayOutputStream();
    encoder = EncoderFactory.get().binaryEncoder( out , null );
    avroFormatter = AvroFormatterFactory.get( avroSchema );
  }

  public AvroMessageWriter( final String schemaString ) throws IOException{
    Schema avroSchema = new Schema.Parser().parse( schemaString );
    writer = new GenericDatumWriter<Object>( avroSchema );
    out = new ByteArrayOutputStream();
    encoder = EncoderFactory.get().binaryEncoder( out , null );
    avroFormatter = AvroFormatterFactory.get( avroSchema );
  }

  public AvroMessageWriter( final InputStream schemaInputStream ) throws IOException{
    Schema avroSchema = new Schema.Parser().parse( schemaInputStream );
    writer = new GenericDatumWriter<Object>( avroSchema );
    out = new ByteArrayOutputStream();
    encoder = EncoderFactory.get().binaryEncoder( out , null );
    avroFormatter = AvroFormatterFactory.get( avroSchema );
  }

  public AvroMessageWriter( final Schema avroSchema ) throws IOException{
    writer = new GenericDatumWriter<Object>( avroSchema );
    out = new ByteArrayOutputStream();
    encoder = EncoderFactory.get().binaryEncoder( out , null );
    avroFormatter = AvroFormatterFactory.get( avroSchema );
  }

  public AvroMessageWriter( final IField schema ) throws IOException{
    Schema avroSchema = AvroSchemaFactory.getAvroSchema( schema );
    writer = new GenericDatumWriter<Object>( avroSchema );
    out = new ByteArrayOutputStream();
    encoder = EncoderFactory.get().binaryEncoder( out , null );
    avroFormatter = AvroFormatterFactory.get( avroSchema );
  }

  @Override
  public byte[] create( final PrimitiveObject obj ) throws IOException{
    out.reset();
    avroFormatter.clear();
    Object writeObj = avroFormatter.write( obj );
    writer.write( writeObj , encoder );
    encoder.flush();
    return out.toByteArray();
  }

  @Override
  public byte[] create( final List<Object> array ) throws IOException{
    out.reset();
    avroFormatter.clear();
    Object writeObj = avroFormatter.write( array );
    writer.write( writeObj , encoder );
    encoder.flush();
    return out.toByteArray();
  }

  @Override
  public byte[] create( final Map<Object,Object> map ) throws IOException{
    out.reset();
    avroFormatter.clear();
    Object writeObj = avroFormatter.write( map );
    writer.write( writeObj , encoder );
    encoder.flush();
    return out.toByteArray();
  }

  @Override
  public byte[] create( final IParser parser ) throws IOException{
    out.reset();
    avroFormatter.clear();
    Object writeObj = avroFormatter.writeParser( null , parser );
    writer.write( writeObj , encoder );
    encoder.flush();
    return out.toByteArray();
  }

  @Override
  public void close() throws IOException{
    out.close();
  }

}
