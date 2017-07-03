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

import org.apache.avro.Schema;

public class AvroArraySchema implements IAvroSchema {

  private final IField schema;
  private final Schema avroSchema;

  public AvroArraySchema( final String fieldName, final Schema avroSchema ) throws IOException{
    this.avroSchema = avroSchema;
    Schema childAvroSchema = avroSchema.getElementType();
    IField childSchema = AvroSchemaFactory.getGeneralSchema( childAvroSchema.getName() , childAvroSchema );

    schema = new ArrayContainerField(fieldName , childSchema );
  }

  public AvroArraySchema( final ArrayContainerField schema ) throws IOException{
    this.schema = schema;
    Schema childSchema = AvroSchemaFactory.getAvroSchema( schema.getField() );
    avroSchema = Schema.createArray( childSchema );
  }

  @Override
  public IField getGeneralSchema() throws IOException{
    return schema;
  }

  @Override
  public Schema getAvroSchema() throws IOException{
    return avroSchema;
  }

}
