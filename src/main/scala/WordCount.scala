import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}

import scala.collection.JavaConversions._


object WordCount extends Configured with Tool {

  class Map extends Mapper[LongWritable, Text, Text, IntWritable] {
    private val one: IntWritable = new IntWritable(1)
    private val word: Text = new Text

    override def map(key: LongWritable, rowLine: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context) {
      val line = rowLine.toString
      if (!line.isEmpty) {
        val tokens: Array[String] = line.split("\\)|\\(|\\=|\\.| |\t|,|:|\\}|\\{|\\/")
        tokens.foreach(f => {
          word.set(f)
          context.write(word, one)
        })
      }
    }
  }

  class Reduce extends Reducer[Text, IntWritable, Text, IntWritable] {
    private val count: IntWritable = new IntWritable()

    override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

      count.set(values.map(m => m.get()).sum)
      context.write(key, count)
    }
  }

  def run(args: Array[String]): Int = {

    import org.apache.hadoop.util.ToolRunner
    if (args.length != 2) {
      System.err.printf("Usage: %s <input><output>", getClass.getSimpleName)
      ToolRunner.printGenericCommandUsage(System.err)
      return -1
    }

    val outPut = new Path(args(1))
    val fs = FileSystem.get(getConf)
    if (fs.exists(outPut))
      fs.delete(outPut, true)

    val conf = super.getConf
    val job = Job.getInstance(conf, "WordCount")

    job.setJarByClass(this.getClass)
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    job.setMapperClass(classOf[Map])
    job.setReducerClass(classOf[Reduce])
    job.setCombinerClass(classOf[Reduce])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    val status = job.waitForCompletion(true)
    if (status) 0 else 1
  }

  def main(args: Array[String]) {
    val conf: Configuration = new Configuration()
    System.exit(ToolRunner.run(conf, this, args))
  }
}