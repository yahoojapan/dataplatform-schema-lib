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

import org.apache.avro.Schema;

public class AvroFormatterFactory{

  public static IAvroFormatter get( final Schema avroSchema ){
    Schema.Type schemaType = avroSchema.getType();

    if( schemaType == Schema.Type.ARRAY ){
      return new AvroArrayFormatter( avroSchema );
    }
    else if( schemaType == Schema.Type.MAP ){
      return new AvroMapFormatter( avroSchema );
    }
    else if( schemaType == Schema.Type.RECORD ){
      return new AvroRecordFormatter( avroSchema );
    }
    else if( schemaType == Schema.Type.UNION ){
      return new AvroUnionFormatter( avroSchema );
    }
    else if( schemaType == Schema.Type.BOOLEAN ){
      return new AvroBooleanFormatter();
    }
    else if( schemaType == Schema.Type.BYTES ){
      return new AvroBytesFormatter();
    }
    else if( schemaType == Schema.Type.DOUBLE ){
      return new AvroDoubleFormatter();
    }
    else if( schemaType == Schema.Type.FLOAT ){
      return new AvroFloatFormatter();
    }
    else if( schemaType == Schema.Type.INT ){
      return new AvroIntegerFormatter();
    }
    else if( schemaType == Schema.Type.LONG ){
      return new AvroLongFormatter();
    }
    else if( schemaType == Schema.Type.STRING ){
      return new AvroStringFormatter();
    }
    else if( schemaType == Schema.Type.NULL ){
      return new AvroNullFormatter();
    }
    else{
      return new AvroNullFormatter();
    }
  }

}
