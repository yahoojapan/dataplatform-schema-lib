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
package jp.co.yahoo.dataplatform.schema.parser;

import java.io.IOException;
import java.io.File;
import java.io.InputStream;

import jp.co.yahoo.dataplatform.schema.design.AvroSchemaFactory;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;

import jp.co.yahoo.dataplatform.schema.design.IField;

public class AvroStreamReader implements IStreamReader {

  private final DataFileStream<Object> stream;

  public AvroStreamReader( final InputStream in , final File avroSchemaFile ) throws IOException{
    Schema avroSchema = new Schema.Parser().parse( avroSchemaFile );
    DatumReader<Object> datumReader = new GenericDatumReader<Object>( avroSchema );
    stream = new DataFileStream( in , datumReader );
  }

  public AvroStreamReader( final InputStream in , final String schemaString ) throws IOException{
    Schema avroSchema = new Schema.Parser().parse( schemaString );
    DatumReader<Object> datumReader = new GenericDatumReader<Object>( avroSchema );
    stream = new DataFileStream( in , datumReader );
  }

  public AvroStreamReader( final InputStream in , final InputStream schemaInputStream ) throws IOException{
    Schema avroSchema = new Schema.Parser().parse( schemaInputStream );
    DatumReader<Object> datumReader = new GenericDatumReader<Object>( avroSchema );
    stream = new DataFileStream( in , datumReader );
  }

  public AvroStreamReader( final InputStream in , final Schema avroSchema ) throws IOException{
    DatumReader<Object> datumReader = new GenericDatumReader<Object>( avroSchema );
    stream = new DataFileStream( in , datumReader );
  }

  public AvroStreamReader( final InputStream in , final IField schema ) throws IOException{
    Schema avroSchema = AvroSchemaFactory.getAvroSchema( schema );
    DatumReader<Object> datumReader = new GenericDatumReader<Object>( avroSchema );
    stream = new DataFileStream( in , datumReader );
  }

  @Override
  public boolean hasNext() throws IOException{
    return stream.hasNext();
  }

  @Override
  public IParser next() throws IOException{
    return AvroObjectToParser.get( stream.next() );
  }

  @Override
  public void close() throws IOException{
    stream.close();
  }

}
