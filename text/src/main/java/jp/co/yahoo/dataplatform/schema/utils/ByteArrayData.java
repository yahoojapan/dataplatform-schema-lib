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
package jp.co.yahoo.dataplatform.schema.utils;

public class ByteArrayData{

	public static final int DEFAULT_BUFFER_SIZE = 1024 * 100;
	public static final int UP_DATA_SIZE_INTERVAL = 1024 * 100;

  public int bufferSizeInterval = UP_DATA_SIZE_INTERVAL;

	private byte[] data;
	private int length = 0;

	public ByteArrayData(){
		data = new byte[ DEFAULT_BUFFER_SIZE ];
		length = 0;
	}

  public ByteArrayData( final int bufferSize ){
		data = new byte[ bufferSize ];
		length = 0;
  }

  public void setBufferInterval( final int bufferSizeInterval ){
    this.bufferSizeInterval = bufferSizeInterval;
  }

	public void clear(){
		length = 0;
	}

	public int getLength(){
		return length;
	}

	public byte[] getBytes(){
		return data;
	}

	@Override
	public int hashCode(){
		int hash = 1;
		for (int i = 0; i < length; i++){
			hash = (31 * hash) + (int)data[i];
		}
		return hash;
	}

	public void append( final ByteArrayData target ){
		if( target == null ){
			return;
		}

		append( target.getBytes() , 0 , target.getLength() );
	}

	public void append( final byte targetByte ){
		checkSize( 1 );
		data[length] = targetByte;
		length += 1;
	}

	public void append( final byte[] targetBytes ){
		append( targetBytes , 0 , targetBytes.length );
	}

	public void append( final byte[] targetBytes , final int targetStart , final int targetLength ){
		checkSize( targetLength );
		System.arraycopy( targetBytes , targetStart , data , length , targetLength );
		length += targetLength;
	}

	private void checkSize( final int addLength ){
		if( data.length < ( length + addLength ) ){
			int newDataSize = data.length + bufferSizeInterval;
			while( newDataSize < ( length + addLength ) ){
				newDataSize += bufferSizeInterval;
			}
			byte[] newBytes = new byte[ newDataSize ];
			System.arraycopy( data , 0 , newBytes , 0 , length );
			data = newBytes;
		}
	}

}
