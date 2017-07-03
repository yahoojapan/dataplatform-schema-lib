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

import java.io.IOException;

import jp.co.yahoo.dataplatform.schema.design.DoubleField;
import jp.co.yahoo.dataplatform.schema.design.LongField;
import jp.co.yahoo.dataplatform.schema.design.NullField;
import jp.co.yahoo.dataplatform.schema.design.StringField;
import jp.co.yahoo.dataplatform.schema.design.IField;
import jp.co.yahoo.dataplatform.schema.design.ArrayContainerField;
import jp.co.yahoo.dataplatform.schema.design.StructContainerField;
import jp.co.yahoo.dataplatform.schema.design.MapContainerField;
import jp.co.yahoo.dataplatform.schema.design.BooleanField;
import jp.co.yahoo.dataplatform.schema.design.BytesField;
import jp.co.yahoo.dataplatform.schema.design.FloatField;
import jp.co.yahoo.dataplatform.schema.design.IntegerField;
import jp.co.yahoo.dataplatform.schema.design.ShortField;

public class TextFormatterFactory{

  public static ITextFormatter get( final IField schema ) throws IOException{
    if( schema instanceof ArrayContainerField ){
      return new TextArrayFormatter( (ArrayContainerField)schema );
    }
    else if( schema instanceof StructContainerField ){
      return new TextStructFormatter( (StructContainerField)schema );
    }
    else if( schema instanceof MapContainerField ){
      return new TextMapFormatter( (MapContainerField)schema );
    }
    else if( schema instanceof BooleanField ){
      return new TextBooleanFormatter();
    }
    else if( schema instanceof BytesField ){
      return new TextBytesFormatter();
    }
    else if( schema instanceof DoubleField){
      return new TextDoubleFormatter();
    }
    else if( schema instanceof FloatField ){
      return new TextFloatFormatter();
    }
    else if( schema instanceof IntegerField ){
      return new TextIntegerFormatter();
    }
    else if( schema instanceof LongField){
      return new TextLongFormatter();
    }
    else if( schema instanceof ShortField ){
      return new TextShortFormatter();
    }
    else if( schema instanceof StringField){
      return new TextStringFormatter();
    }
    else if( schema instanceof NullField){
      return new TextNullFormatter();
    }
    else {
      return new TextNullFormatter();
    }
  }

}
