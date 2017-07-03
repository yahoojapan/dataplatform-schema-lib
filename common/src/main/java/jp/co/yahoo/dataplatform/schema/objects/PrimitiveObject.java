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

public interface PrimitiveObject{

  public Object get() throws IOException;

  public String getString() throws IOException;

  public byte[] getBytes() throws IOException;

  public byte getByte() throws IOException;

  public short getShort() throws IOException;

  public int getInt() throws IOException;

  public long getLong() throws IOException;

  public float getFloat() throws IOException;

  public double getDouble() throws IOException;

  public boolean getBoolean() throws IOException;

  public void setString( final String data ) throws IOException;

  public void setBytes( final byte[] data ) throws IOException;

  public void setBytes( final byte[] data , final int start , final int length ) throws IOException;

  public void setByte( final byte data ) throws IOException;

  public void setShort( final short data ) throws IOException;

  public void setInt( final int data ) throws IOException;

  public void setLong( final long data ) throws IOException;

  public void setFloat( final float data ) throws IOException;

  public void setDouble( final double data ) throws IOException;

  public void setBoolean( final boolean data ) throws IOException;

  public void set( final PrimitiveObject data ) throws IOException;

  public void clear() throws IOException;

  public PrimitiveType getPrimitiveType();

}
