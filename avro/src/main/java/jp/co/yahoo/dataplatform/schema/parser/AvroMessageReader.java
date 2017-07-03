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
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import jp.co.yahoo.dataplatform.schema.design.IField;

public class AvroMessageReader implements IMessageReader {

  private final DatumReader<Object> datumReader;
  private BinaryDecoder decoder = null;

  public AvroMessageReader( final File avroSchemaFile ) throws IOException{
    Schema avroSchema = new Schema.Parser().parse( avroSchemaFile );
    this.datumReader = new GenericDatumReader<>( avroSchema );
  }

  public AvroMessageReader( final String schemaString ) throws IOException{
    Schema avroSchema = new Schema.Parser().parse( schemaString );
    this.datumReader = new GenericDatumReader<>( avroSchema);
  }

  public AvroMessageReader( final InputStream schemaInputStream ) throws IOException{
    Schema avroSchema = new Schema.Parser().parse( schemaInputStream );
    this.datumReader = new GenericDatumReader<>( avroSchema );
  }

  public AvroMessageReader( final Schema avroSchema ) throws IOException{
    this.datumReader = new GenericDatumReader<>( avroSchema );
  }

  public AvroMessageReader( final IField schema ) throws IOException{
    Schema avroSchema = AvroSchemaFactory.getAvroSchema( schema );
    this.datumReader = new GenericDatumReader<>( avroSchema );
  }

  public IParser create( final Object record ) throws IOException{
    return AvroObjectToParser.get( record );
  }

  @Override
  public IParser create( final byte[] message ) throws IOException{
    return create( message , 0 , message.length );
  }

  @Override
  public IParser create( final byte[] message , final int start , final int length ) throws IOException{
    decoder = DecoderFactory.get().binaryDecoder(message, start, length, null);
    Object record = datumReader.read( null , decoder );
    return create( record );
  }

}
