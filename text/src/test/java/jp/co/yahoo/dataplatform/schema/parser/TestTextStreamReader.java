package jp.co.yahoo.dataplatform.schema.parser;

import jp.co.yahoo.dataplatform.schema.design.ArrayContainerField;
import jp.co.yahoo.dataplatform.schema.design.LongField;
import jp.co.yahoo.dataplatform.schema.utils.Properties;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.testng.Assert.*;

/**
 * Created by ssaitou on 2017/06/27.
 */
public class TestTextStreamReader {

    @Test
    public void T_reader() throws Exception {
        Properties properties = new Properties();
        properties.set( "delimiter" , "0x2c" );
        ArrayContainerField schema = new ArrayContainerField( "array" , new LongField( "array_value" ) , properties );

        byte[] dummyData = "100,200,300\n1,2,3\n10,20,30\n999,999,999\n".getBytes("UTF-8");
        InputStream in = new ByteArrayInputStream(dummyData);

        TextStreamReader reader = new TextStreamReader(in, schema);
        while (reader.hasNext()){
            IParser out = reader.next();
            System.out.println(out.get(2).getString());
        }

    }
}