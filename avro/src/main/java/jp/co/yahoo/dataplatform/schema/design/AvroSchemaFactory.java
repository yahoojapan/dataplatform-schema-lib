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

public class AvroSchemaFactory{

  public static IField getGeneralSchema( final String fieldName , final Schema avroSchema ) throws IOException{
    Schema.Type schemaType = avroSchema.getType();

    if( schemaType == Schema.Type.ARRAY ){
      return new AvroArraySchema( fieldName, avroSchema ).getGeneralSchema();
    }
    else if( schemaType == Schema.Type.MAP ){
      return new AvroMapSchema( fieldName, avroSchema ).getGeneralSchema();
    }
    else if( schemaType == Schema.Type.RECORD ){
      return new AvroRecordSchema( avroSchema ).getGeneralSchema();
    }
    else if( schemaType == Schema.Type.UNION ){
      List<Schema> childSchemas = avroSchema.getTypes();
      if( childSchemas.size() == 2 ){
        Schema childSchema0 = childSchemas.get(0);
        Schema childSchema1 = childSchemas.get(1);
        if( childSchema0.getType() == Schema.Type.NULL && childSchema1.getType() != Schema.Type.NULL ){
          return getGeneralSchema( fieldName , childSchema1 );
        }
        else if( childSchema0.getType() != Schema.Type.NULL && childSchema1.getType() == Schema.Type.NULL ){
          return getGeneralSchema( fieldName , childSchema0 );
        }
      }
      return new AvroUnionSchema( avroSchema ).getGeneralSchema();
    }
    else if( schemaType == Schema.Type.BOOLEAN ){
      return new BooleanField( fieldName );
    }
    else if( schemaType == Schema.Type.BYTES ){
      return new BytesField( fieldName );
    }
    else if( schemaType == Schema.Type.DOUBLE ){
      return new DoubleField( fieldName );
    }
    else if( schemaType == Schema.Type.FLOAT ){
      return new FloatField( fieldName );
    }
    else if( schemaType == Schema.Type.INT ){
      return new IntegerField( fieldName );
    }
    else if( schemaType == Schema.Type.LONG ){
      return new LongField( fieldName );
    }
    else if( schemaType == Schema.Type.STRING ){
      return new StringField( fieldName );
    }
    else if( schemaType == Schema.Type.NULL ){
      return new NullField( fieldName );
    }
    else{
      return new NullField( fieldName );
    }
  }

  public static Schema getAvroSchema( final IField schema ) throws IOException{
    return getAvroSchema( schema , true );
  }

  public static Schema getAvroSchema( final IField schema , final boolean nullFlag ) throws IOException{
    if( schema instanceof ArrayContainerField ){
      return new AvroArraySchema( (ArrayContainerField)schema ).getAvroSchema();
    }
    else if( schema instanceof MapContainerField ){
      return new AvroMapSchema( (MapContainerField)schema ).getAvroSchema();
    }
    else if( schema instanceof StructContainerField ){
      return new AvroRecordSchema( (StructContainerField)schema ).getAvroSchema();
    }
    else if( schema instanceof UnionField ){
      return new AvroUnionSchema( (UnionField)schema ).getAvroSchema();
    }
    else if( schema instanceof BooleanField ){
      if( nullFlag ){
        List<Schema> childList = new ArrayList<Schema>();
        childList.add( Schema.create( Schema.Type.BOOLEAN ) );
        childList.add( Schema.create( Schema.Type.NULL ) );
        return Schema.createUnion( childList );
      }
      else{
        return Schema.create( Schema.Type.BOOLEAN );
      }
    }
    else if( schema instanceof BytesField ){
      if( nullFlag ){
        List<Schema> childList = new ArrayList<Schema>();
        childList.add( Schema.create( Schema.Type.BYTES ) );
        childList.add( Schema.create( Schema.Type.NULL ) );
        return Schema.createUnion( childList );
      }
      else{
        return Schema.create( Schema.Type.BYTES );
      }
    }
    else if( schema instanceof DoubleField ){
      if( nullFlag ){
        List<Schema> childList = new ArrayList<Schema>();
        childList.add( Schema.create( Schema.Type.DOUBLE ) );
        childList.add( Schema.create( Schema.Type.NULL ) );
        return Schema.createUnion( childList );
      }
      else{
        return Schema.create( Schema.Type.DOUBLE );
      }
    }
    else if( schema instanceof FloatField ){
      if( nullFlag ){
        List<Schema> childList = new ArrayList<Schema>();
        childList.add( Schema.create( Schema.Type.FLOAT ) );
        childList.add( Schema.create( Schema.Type.NULL ) );
        return Schema.createUnion( childList );
      }
      else{
        return Schema.create( Schema.Type.FLOAT );
      }
    }
    else if( schema instanceof IntegerField ){
      if( nullFlag ){
        List<Schema> childList = new ArrayList<Schema>();
        childList.add( Schema.create( Schema.Type.INT ) );
        childList.add( Schema.create( Schema.Type.NULL ) );
        return Schema.createUnion( childList );
      }
      else{
        return Schema.create( Schema.Type.INT );
      }
    }
    else if( schema instanceof LongField ){
      if( nullFlag ){
        List<Schema> childList = new ArrayList<Schema>();
        childList.add( Schema.create( Schema.Type.LONG ) );
        childList.add( Schema.create( Schema.Type.NULL ) );
        return Schema.createUnion( childList );
      }
      else{
        return Schema.create( Schema.Type.LONG );
      }
    }
    else if( schema instanceof ShortField ){
      if( nullFlag ){
        List<Schema> childList = new ArrayList<Schema>();
        childList.add( Schema.create( Schema.Type.INT ) );
        childList.add( Schema.create( Schema.Type.NULL ) );
        return Schema.createUnion( childList );
      }
      else{
        return Schema.create( Schema.Type.INT );
      }
    }
    else if( schema instanceof StringField){
      if( nullFlag ){
        List<Schema> childList = new ArrayList<Schema>();
        childList.add( Schema.create( Schema.Type.STRING ) );
        childList.add( Schema.create( Schema.Type.NULL ) );
        return Schema.createUnion( childList );
      }
      else{
        return Schema.create( Schema.Type.STRING );
      }
    }
    else if( schema instanceof NullField ){
      return Schema.create( Schema.Type.NULL );
    }
    else{
      return Schema.create( Schema.Type.NULL );
    }
  }

}
