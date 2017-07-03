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
package jp.co.yahoo.dataplatform.schema.objects;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.AvroTypeException;

public class AvroObjectToPrimitiveObject{

  private static final GenericData genericUtil = new GenericData();

  public static PrimitiveObject get( final Object obj ) throws IOException{
    Schema.Type schemaType = null;
    try{
      schemaType = genericUtil.induce( obj ).getType();
    }catch( AvroTypeException e ){
      return new StringObj( obj.toString() );
    }

    if( schemaType == Schema.Type.BOOLEAN ){
      return new BooleanObj( (Boolean)obj );
    }
    else if( schemaType == Schema.Type.BYTES ){
      return new BytesObj( ((ByteBuffer)obj).array() );
    }
    else if( schemaType == Schema.Type.DOUBLE ){
      return new DoubleObj( (Double)obj );
    }
    else if( schemaType == Schema.Type.FLOAT ){
      return new FloatObj( (Float)obj );
    }
    else if( schemaType == Schema.Type.INT ){
      return new IntegerObj( (Integer)obj );
    }
    else if( schemaType == Schema.Type.LONG ){
      return new LongObj( (Long)obj );
    }
    else if( schemaType == Schema.Type.STRING ){
      return new StringObj( obj.toString() );
    }
    else if( schemaType == Schema.Type.NULL ){
      return NullObj.getInstance();
    }
    else{
      return new StringObj( obj.toString() );
    }
  }


}
