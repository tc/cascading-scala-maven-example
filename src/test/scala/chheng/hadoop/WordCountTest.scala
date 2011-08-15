package chheng.hadoop

import org.junit.{Test, Assert}
import java.io._
import org.apache.commons.io._

class WordCountTest{

  @Test
  def testExamples{
    val input1 = "src/test/resources/input/wordcount.txt"
    val output = "src/test/resources/output"

    FileUtils.deleteDirectory(new File(output))

    WordCount.main(Array(input1, output))

    val result = scala.io.Source.fromFile(output + "/part-r-00000").getLines.toList

    assert(result.contains("was	20"))
  }
}
