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

public class PrimitiveObjectToAvroObject{

  public static Object get( final Schema.Type schemaType , final PrimitiveObject obj ) throws IOException{
    if( schemaType == Schema.Type.BOOLEAN ){
      return obj.getBoolean();
    }
    else if( schemaType == Schema.Type.BYTES ){
      byte[] bytes = obj.getBytes();
      ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
      byteBuffer.put(bytes, 0, bytes.length);
      return byteBuffer;
    }
    else if( schemaType == Schema.Type.DOUBLE ){
      return obj.getDouble();
    }
    else if( schemaType == Schema.Type.FLOAT ){
      return obj.getFloat();
    }
    else if( schemaType == Schema.Type.INT ){
      return obj.getInt();
    }
    else if( schemaType == Schema.Type.LONG ){
      return obj.getLong();
    }
    else if( schemaType == Schema.Type.STRING ){
      return obj.getString();
    }
    else if( schemaType == Schema.Type.NULL ){
      return null;
    }
    else{
      return obj.getString();
    }
  }

}
