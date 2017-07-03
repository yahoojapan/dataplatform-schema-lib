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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import jp.co.yahoo.dataplatform.schema.objects.IntegerObj;
import jp.co.yahoo.dataplatform.schema.objects.BooleanObj;
import jp.co.yahoo.dataplatform.schema.objects.StringObj;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class TestAvroBytesFormatter{

  //---------------------------------------
  // DataProvider
  //---------------------------------------

  @DataProvider( name = "testcase_write" )
  public Object[][] testcase_write() throws UnsupportedEncodingException{
    return new Object[][]{
      {"hoge".getBytes( "UTF-8" ) , "hoge".getBytes( "UTF-8" )} ,
      {"piyo" , "piyo".getBytes( "UTF-8" )} ,
      {new BooleanObj( true ) , "true".getBytes( "UTF-8" )} ,
      {new IntegerObj( 1 ) , "1".getBytes( "UTF-8" )} ,
      {new StringObj( "string" ) , "string".getBytes( "UTF-8" )} ,
    };
  }

  @DataProvider( name = "testcase_writeBytesToByteBuffer" )
  public Object[][] testcase_writeBytesToByteBuffer() throws UnsupportedEncodingException{
    return new Object[][]{
      {"hoge".getBytes( "UTF-8" )} ,
      {"piyo".getBytes( "UTF-8" )} ,
      {"true".getBytes( "UTF-8" )} ,
      {"1".getBytes( "UTF-8" )} ,
      {"string".getBytes( "UTF-8" )} ,
    };
  }

  //---------------------------------------
  // Test
  //---------------------------------------

  @Test( dataProvider = "testcase_write" )
  public void T_write( final Object inputObj, final byte[] expectedObj ) throws IOException{
    AvroBytesFormatter avroBytesFormatter = new AvroBytesFormatter();
    Object byteBuffer = avroBytesFormatter.write( inputObj );

    assertTrue( byteBuffer instanceof ByteBuffer ); // check Object type

    assertTrue( Arrays.equals( expectedObj, (( ByteBuffer ) byteBuffer).array() ) );

    assertEquals( 0, (( ByteBuffer ) byteBuffer).position() ); // expect that ByteBuffer's position is 0

    assertEquals( expectedObj.length, (( ByteBuffer ) byteBuffer).limit() );
    for( int index = 0; index < (( ByteBuffer ) byteBuffer).limit() && index < expectedObj.length; ++index ){
      assertEquals( expectedObj[index], (( ByteBuffer ) byteBuffer).get( index ) );
    }
  }

  @Test( dataProvider = "testcase_writeBytesToByteBuffer" )
  public void T_writeBytesToByteBuffer( final byte[] inputAndExpectedBytes ) throws IOException{
    AvroBytesFormatter avroBytesFormatter = new AvroBytesFormatter();

    ByteBuffer byteBuffer = avroBytesFormatter.writeBytesToByteBuffer( inputAndExpectedBytes );

    assertEquals( inputAndExpectedBytes.length, byteBuffer.limit() );

    assertEquals( 0, byteBuffer.position() ); // expect that ByteBuffer's position is 0

    assertTrue( Arrays.equals( inputAndExpectedBytes, byteBuffer.array() ) );

    for( int index = 0; index < byteBuffer.limit() && index < inputAndExpectedBytes.length; ++index ){
      assertEquals( inputAndExpectedBytes[index], byteBuffer.get( index ) );
    }
  }
}
