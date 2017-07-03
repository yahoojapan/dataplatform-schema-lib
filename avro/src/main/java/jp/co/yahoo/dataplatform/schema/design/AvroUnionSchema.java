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

import org.apache.avro.Schema;

public class AvroUnionSchema implements IAvroSchema {

  private final UnionField schema;
  private final Schema avroSchema;

  public AvroUnionSchema( final Schema avroSchema ) throws IOException{
    this.avroSchema = avroSchema;
    schema = new UnionField( avroSchema.getName() );

    for( Schema childSchema : avroSchema.getTypes() ){
      IField childField = AvroSchemaFactory.getGeneralSchema( avroSchema.getName() , childSchema );
      schema.set( childField );
    }
  }

  public AvroUnionSchema( final UnionField schema ) throws IOException{
    this.schema = schema;

    List<Schema> childList = new ArrayList<Schema>();
    for( String key : schema.getKeys() ){
      childList.add( AvroSchemaFactory.getAvroSchema( schema.get( key ) , false ) );
    }

    avroSchema = Schema.createUnion( childList );
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
