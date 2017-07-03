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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class OrcStreamReader implements IStreamReader {

  private final ObjectInspector inspector;
  private final RecordReader rows;

  private Object row = null;

  public OrcStreamReader( final Configuration config, final Path path ) throws IOException{
    Reader reader = OrcFile.createReader( path , OrcFile.readerOptions(config) );
    inspector = reader.getObjectInspector();
    rows = reader.rows();
  }

  @Override
  public boolean hasNext() throws IOException{
    return rows.hasNext();
  }

  @Override
  public IParser next() throws IOException{
    row = rows.next( row );
    return HiveParserFactory.get( inspector , row );
  }

  @Override
  public void close() throws IOException{
    rows.close();
  }

}
