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

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

public class HiveNullParser implements IHiveParser {

  @Override
  public void setObject( final Object row ) throws IOException{
  }

  @Override
  public PrimitiveObject get(final String key ) throws IOException{
    return null;
  }

  @Override
  public PrimitiveObject get( final int index ) throws IOException{
    return null;
  }

  @Override
  public IParser getParser( final String key ) throws IOException{
    return new HiveNullParser();
  }

  @Override
  public IParser getParser( final int index ) throws IOException{
    return new HiveNullParser();
  }

  @Override
  public String[] getAllKey() throws IOException{
    return new String[0];
  }

  @Override
  public boolean containsKey( final String key ) throws IOException{
    return false;
  }

  @Override
  public int size() throws IOException{
    return 0;
  }

  @Override
  public boolean isArray() throws IOException{
    return false;
  }

  @Override
  public boolean isMap() throws IOException{
    return false;
  }

  @Override
  public boolean isStruct() throws IOException{
    return false;
  }

  @Override
  public boolean hasParser( final int index ) throws IOException{
    return false;
  }

  @Override
  public boolean hasParser( final String key ) throws IOException{
    return false;
  }

  @Override
  public Object toJavaObject() throws IOException{
    return null;
  }

}
