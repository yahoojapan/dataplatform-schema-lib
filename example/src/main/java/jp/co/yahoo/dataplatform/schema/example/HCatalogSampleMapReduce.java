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
package jp.co.yahoo.dataplatform.schema.example;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import org.apache.hive.hcatalog.mapreduce.*;

public class HCatalogSampleMapReduce extends Configured implements Tool{

  public int run( final String[] args ) throws Exception{
    String dbName = args[0];
    String tableName = args[1];
    String outputPath = args[2];
    String hiveArchives = args[3];
    String libjars = args[4];

    Configuration config = new Configuration();

    HCatInputFormat.setInput( config , dbName , tableName );

    config.set( "mapreduce.application.classpath" , String.format( "%s:%s:%s" , config.get( "mapreduce.application.classpath" ) , "$PWD/hive.tar.gz/hive/lib/*" , "$PWD/hive.tar.gz/hive/hcatalog/share/hcatalog/*" ) );
    config.set( "mapred.compress.map.output" , "true" );
    config.set( "mapred.map.output.compression.codec" , "org.apache.hadoop.io.compress.GzipCodec" );

    FileSystem fs = FileSystem.get( config );
    addCacheArchive( hiveArchives , config , fs );
    addCacheFiles( libjars , config );
    
    Job job = new Job( config , "HCatalog Mapreduce sample." );

    job.setMapOutputKeyClass( Text.class );
    job.setMapOutputValueClass( LongWritable.class );
    job.setOutputKeyClass( Text.class );
    job.setOutputValueClass( LongWritable.class );

    job.setMapperClass( HCatalogSampleMap.class );
    job.setCombinerClass( HCatalogSampleReduce.class );
    job.setReducerClass( HCatalogSampleReduce.class );
    job.setNumReduceTasks( 150 );

    job.setInputFormatClass( HCatInputFormat.class );

    job.setOutputFormatClass( TextOutputFormat.class );
    FileOutputFormat.setOutputPath( job , new Path( outputPath ) );

    return ( job.waitForCompletion( true ) ? 0 : 1 );
  }

  public void addCacheFiles( final String filesString , final Configuration conf ) throws IOException{
    for( String file : filesString.split( "," ) ){
      DistributedCache.addFileToClassPath( new Path( file ) , conf );
    }
  }

  public void addCacheArchive( final String arcString , final Configuration conf , final FileSystem fs ) throws IOException{
    for( String file : arcString.split( "," ) ){
      DistributedCache.addArchiveToClassPath( new Path( file ) , conf , fs );
    }
  }

  public static void main( final String[] args ) throws Exception{
    if( args.length != 5 ){
      throw new IOException( "HCatalogSampleMapReduce <DB_NAME> <TABLE_NAME> <OutputPath> <HIVE_ARCHIVE> <LIBJARS>" );
    }
    System.exit( ToolRunner.run( new HCatalogSampleMapReduce() , args ) ); 
  }

}
