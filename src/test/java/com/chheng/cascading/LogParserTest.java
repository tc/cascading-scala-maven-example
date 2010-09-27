package com.chheng.cascading;

import org.junit.Test;

/**
 * User: @tommychheng
 * Date: Sep 13, 2010
 * Time: 9:16:19 PM
 */
public class LogParserTest {

    @Test
    public void testMain(){
        String[] args = new String[]{"src/test/resources/apache.10.txt",
                                     "src/test/resources/output"};
        LogParser.main(args);
    }
}
