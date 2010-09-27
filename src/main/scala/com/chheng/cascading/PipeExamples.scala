package com.chheng.cascading

import cascading.tuple.Fields
import cascading.scheme.{TextLine, TextDelimited}
import java.util.Properties
import cascading.tap.{SinkMode, Hfs}
import cascading.flow.{Flow, FlowConnector}
import cascading.pipe.Each
import cascading.operation.Identity

/**
 * 
 * User: @tommychheng
 * Date: Sep 13, 2010
 * Time: 9:36:10 PM
 *
 *
 */

object PipeExamples{
  def main(args: Array[String]) {
    val properties = new Properties()
    FlowConnector.setApplicationJarClass( properties, PipeExamples.getClass )
    val flowConnector = new FlowConnector(properties)

    val fileOneFields = new Fields("file1 field1", "file1 field2", "file1 field3")
    val sourcePath = args(0)
    val sourceScheme = new TextDelimited(fileOneFields, false, "\t")
    val sourceTap = new Hfs(sourceScheme, sourcePath, true)

    val fileTwoFields = new Fields("file2 field1", "file2 field2", "file2 field3")
    val sourceTwoPath = args(1)
    val sourceTwoScheme = new TextDelimited(fileTwoFields, false, "\t")
    val sourceTwoTap = new Hfs(sourceTwoScheme, sourceTwoPath, true)

    val sinkPath = args(2)
    val sinkTap = new Hfs(new TextLine(), sinkPath, SinkMode.REPLACE )

    //BEGIN APPLICATION CODE

    val pipe = new Each("identity", new Fields( "file1 field1"), new Identity())

    //END APPLICATION CODE

    val flow = flowConnector.connect( sourceTap, sinkTap, pipe )

    // optionally print out the parsedLogFlow to a DOT file for import into a graphics package
    flow.writeDOT( "pipeExamples.dot" )

    flow.complete()
  }
}