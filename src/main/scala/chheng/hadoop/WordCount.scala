package chheng.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser

import scala.collection.JavaConversions._

// using from
// https://github.com/cageface/scala-hadoop-example/blob/master/WordCount.scala
class TokenizerMapper extends Mapper[Object,Text,Text,IntWritable] {
  val one = new IntWritable(1)
  val word = new Text

  override def map(key:Object,
                   value:Text, 
                   context:Mapper[Object, Text, Text, IntWritable]#Context) = {
    value.toString().split("\\s+").map{ t =>
      word.set(t)
      context.write(word, one)
    }
  }
}

class IntSumReducer extends Reducer[Text,IntWritable,Text,IntWritable] {

  override def reduce(key:Text,
                      values:java.lang.Iterable[IntWritable], 
                      context:Reducer[Text, IntWritable, Text, IntWritable]#Context) = {

    val sum = values.foldLeft(0) { (t,i) => t + i.get }
    context.write(key, new IntWritable(sum))
  }
}

object WordCount {
  def main(args:Array[String]):Int = {
    val conf = new Configuration()

    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    if (otherArgs.length != 2) {
      println("Usage: wordcount <in> <out>")
      return 2
    }

    val job = new Job(conf, "word count")
    job.setJarByClass(classOf[TokenizerMapper])
    job.setMapperClass(classOf[TokenizerMapper])
    job.setCombinerClass(classOf[IntSumReducer])
    job.setReducerClass(classOf[IntSumReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path((args(1))))

    if (job.waitForCompletion(true)) 0 else 1
  }
}
