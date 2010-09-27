package com.chheng.cascading

import org.junit.Test

/**
 * 
 * User: @tommychheng
 * Date: Sep 13, 2010
 * Time: 9:36:41 PM
 *
 *
 */

class PipeExamplesTest{

  @Test
  def testExamples{
    val input1 = "src/test/resources/pipe_examples_file1.txt"
    val input2 = "src/test/resources/pipe_examples_file2.txt"
    val output = "src/test/resources/output"

    PipeExamples.main(Array(input1, input2, output))

    val result = scala.io.Source.fromFile(output + "/part-00000").getLines.toList

    result.foreach(println)
  }

}