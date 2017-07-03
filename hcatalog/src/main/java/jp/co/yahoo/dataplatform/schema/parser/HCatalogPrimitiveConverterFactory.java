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

import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;

public class HCatalogPrimitiveConverterFactory{

  public static IHCatalogPrimitiveConverter get( final HCatFieldSchema hCatFieldSchema ) throws IOException{
    if( hCatFieldSchema.getCategory() == HCatFieldSchema.Category.ARRAY ){
      return new HCatalogNestPrimitiveConverter();
    }
    else if( hCatFieldSchema.getCategory() == HCatFieldSchema.Category.MAP ){
      return new HCatalogNestPrimitiveConverter();
    }
    else if( hCatFieldSchema.getCategory() == HCatFieldSchema.Category.STRUCT ){
      return new HCatalogNestPrimitiveConverter();
    }
    else if( hCatFieldSchema.getCategory() == HCatFieldSchema.Category.PRIMITIVE ){
      PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo)hCatFieldSchema.getTypeInfo();
      if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.BINARY ){
        return new HCatalogBytesPrimitiveConverter();
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN ){
        return new HCatalogBooleanPrimitiveConverter();
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.BYTE ){
        return new HCatalogBytePrimitiveConverter();
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.DOUBLE ){
        return new HCatalogDoublePrimitiveConverter();
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.FLOAT ){
        return new HCatalogFloatPrimitiveConverter();
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT ){
        return new HCatalogIntegerPrimitiveConverter();
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.LONG ){
        return new HCatalogLongPrimitiveConverter();
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.SHORT ){
        return new HCatalogShortPrimitiveConverter();
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING ){
        return new HCatalogStringPrimitiveConverter();
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.DATE ){
        return new HCatalogDefaultPrimitiveConverter();
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP ){
        return new HCatalogDefaultPrimitiveConverter();
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.VOID ){
        return new HCatalogDefaultPrimitiveConverter();
      }
      else if( primitiveTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.UNKNOWN ){
        return new HCatalogDefaultPrimitiveConverter();
      }
      else {
        return new HCatalogDefaultPrimitiveConverter();
      }
    }
    else{
      throw new IOException( "Unknown HCatalog field type : " + hCatFieldSchema.toString() );
    }
  }

}
