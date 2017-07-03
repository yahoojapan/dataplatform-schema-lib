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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

@Deprecated
public class OrcPrimitiveConverterFactory{

  public static IOrcPrimitiveConverter get( final ObjectInspector objectInspector ){

    switch( objectInspector.getCategory() ){
      case PRIMITIVE:
        PrimitiveObjectInspector primitiveInspector = (PrimitiveObjectInspector)objectInspector;
        switch( primitiveInspector.getPrimitiveCategory() ){
          case BINARY:
            return new OrcBytesPrimitiveConverter( (BinaryObjectInspector)objectInspector );
          case BOOLEAN:
            return new OrcBooleanPrimitiveConverter( (BooleanObjectInspector)objectInspector );
          case BYTE:
            return new OrcBytePrimitiveConverter( (ByteObjectInspector)objectInspector );
          case DOUBLE:
            return new OrcDoublePrimitiveConverter( (DoubleObjectInspector)objectInspector );
          case FLOAT:
            return new OrcFloatPrimitiveConverter( (FloatObjectInspector)objectInspector );
          case INT:
            return new OrcIntegerPrimitiveConverter( (IntObjectInspector)objectInspector );
          case LONG:
            return new OrcLongPrimitiveConverter( (LongObjectInspector)objectInspector );
          case SHORT:
            return new OrcShortPrimitiveConverter( (ShortObjectInspector)objectInspector );
          case STRING:
            return new OrcStringPrimitiveConverter( (StringObjectInspector)objectInspector );
          case DATE:
          case TIMESTAMP:
          case VOID:
          case UNKNOWN:
          default:
            return new OrcDefaultPrimitiveConverter();
        }
      default :
        return new OrcDefaultPrimitiveConverter();
    }

/*
    if( objectInspector.getCategory()  == ObjectInspector.Category.PRIMITIVE ){
      PrimitiveObjectInspector primitiveInspector = (PrimitiveObjectInspector)objectInspector;
      if( primitiveInspector.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.BINARY ){
        return new OrcBytesPrimitiveConverter( (BinaryObjectInspector)objectInspector );
      }
      else if( primitiveInspector.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN ){
        return new OrcBooleanPrimitiveConverter( (BooleanObjectInspector)objectInspector );
      }
      else if( primitiveInspector.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.BYTE ){
        return new OrcBytePrimitiveConverter( (ByteObjectInspector)objectInspector );
      }
      else if( primitiveInspector.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.DOUBLE ){
        return new OrcDoublePrimitiveConverter( (DoubleObjectInspector)objectInspector );
      }
      else if( primitiveInspector.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.FLOAT ){
        return new OrcFloatPrimitiveConverter( (FloatObjectInspector)objectInspector );
      }
      else if( primitiveInspector.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT ){
        return new OrcIntegerPrimitiveConverter( (IntObjectInspector)objectInspector );
      }
      else if( primitiveInspector.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.LONG ){
        return new OrcLongPrimitiveConverter( (LongObjectInspector)objectInspector );
      }
      else if( primitiveInspector.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.SHORT ){
        return new OrcShortPrimitiveConverter( (ShortObjectInspector)objectInspector );
      }
      else if( primitiveInspector.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING ){
        return new OrcStringPrimitiveConverter( (StringObjectInspector)objectInspector );
      }
      else if( primitiveInspector.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.DATE ){
        return new OrcDefaultPrimitiveConverter();
      }
      else if( primitiveInspector.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP ){
        return new OrcDefaultPrimitiveConverter();
      }
      else if( primitiveInspector.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.VOID ){
        return new OrcDefaultPrimitiveConverter();
      }
      else if( primitiveInspector.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.UNKNOWN ){
        return new OrcDefaultPrimitiveConverter();
      }
      else {
        return new OrcDefaultPrimitiveConverter();
      }
    }

    return new OrcDefaultPrimitiveConverter();
*/
  }

}
