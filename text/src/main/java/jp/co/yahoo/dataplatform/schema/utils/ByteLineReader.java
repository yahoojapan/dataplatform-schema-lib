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

import java.io.IOException;

import java.io.InputStream;

public class ByteLineReader {
    private static final int INPUT_BUFFER_SIZE = 1024 *  1024 *  1;
    /** \n の改行コード */
    private static final byte N_END = '\n';
    /** \r の改行コード */
    private static final byte R_END = '\r';

    private final ByteArrayData bufferBytes = new ByteArrayData();

    private final InputStream in;
    private final byte[] buffer;
    private int bufferOffset;
    private int bufferSize;
    boolean crFlag=false;

    /**
     * コストラクタ.
     *
     * @param in 入力となる InputStream.
     * @throws IOException InputStream 内で発生したものをそのまま渡します.
     */
    public ByteLineReader( final InputStream in ) throws IOException{
      buffer = new byte[INPUT_BUFFER_SIZE];
      if( in == null ){
        throw new IOException("InputStream is null");
      }
      this.in = in;
      bufferOffset = 0;
    }

    /**
     * InputStream をクローズするための関数.
     */
    public void close() throws IOException{
      in.close();
    }

    /**
     * １行を入力するための関数.
     *
     * @return 入力した１行の byte 長を返す.読み込みが終了している場合には -1 が返る.
     */
    public int readLine() throws IOException{
      int copyLength = 0;
      bufferBytes.clear();
      while( bufferSize != -1 ){
        if( bufferSize <= bufferOffset ){

          bufferBytes.append( buffer , ( bufferOffset - copyLength ) , copyLength );
          copyLength = 0;

          bufferSize = in.read( buffer );
          bufferOffset = 0;
          continue;
        }

        if( buffer[bufferOffset] == N_END ){
          if( bufferBytes.getLength() == 0 && crFlag ){
            crFlag = false;
            continue;
          }
          crFlag = false;
          break;
        }
        else if( buffer[bufferOffset] == R_END ){
          crFlag = true;
          break;
        }

        bufferOffset++;
        copyLength++;

      }

      if( bufferSize == -1 && bufferBytes.getLength() == 0 ){
        return -1;
      }

      bufferBytes.append( buffer , ( bufferOffset - copyLength ) , copyLength );
      bufferOffset++;

      return bufferBytes.getLength();
    }

    public boolean hasNext() throws IOException {
        if (bufferSize == -1 ) return false;
        if( bufferSize <= bufferOffset ) {
            bufferSize = in.read(buffer, 0 , buffer.length);
            bufferOffset = 0;
            return bufferSize != -1;
        }
        return true;
    }

    public byte[] get(){
      return bufferBytes.getBytes();
    }
}
