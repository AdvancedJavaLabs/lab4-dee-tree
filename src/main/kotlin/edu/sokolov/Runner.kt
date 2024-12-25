package edu.sokolov

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import kotlin.system.exitProcess

class Runner {
    companion object {
        const val VERBOSE = true

        @JvmStatic
        fun main(args: Array<String>) {
            val tmpDir = Path("/tmp/reduced")
            val job1 = reducer(Path(args[0]), tmpDir)
            if (!job1.waitForCompletion(VERBOSE)) exitProcess(1)

            val job2 = sorter(tmpDir, Path(args[1]))
            if (!job2.waitForCompletion(VERBOSE)) exitProcess(1)

        }

        private fun reducer(input: Path, output: Path): Job {
            val conf = Configuration().apply {
                this.set(TextOutputFormat.SEPARATOR, ",")
            }
            val job = Job.getInstance(conf)

            job.setJarByClass(Runner::class.java)
            job.mapperClass = SalesBook.LinesHandler::class.java

            job.mapOutputKeyClass = Text::class.java
            job.mapOutputValueClass = CompositeKey::class.java

            job.reducerClass = SalesBook.SalesReducer::class.java

            job.outputKeyClass = Text::class.java
            job.outputValueClass = CompositeKey::class.java

            FileInputFormat.addInputPath(job, input)
            FileOutputFormat.setOutputPath(job, output)
            output.getFileSystem(conf).delete(output)
            return job
        }

        private fun sorter(input: Path, output: Path): Job {
            val conf = Configuration().apply {
//                this.set(TextOutputFormat.SEPARATOR, ",")
            }
            val job = Job.getInstance(conf)

            job.setJarByClass(Runner::class.java)
            job.mapperClass = SalesSorter.LinesHandler::class.java

            job.mapOutputKeyClass = SalesData::class.java
            job.mapOutputValueClass = Text::class.java

            job.reducerClass = SalesSorter.SalesReducer::class.java

            job.outputKeyClass = SalesData::class.java
            job.outputValueClass = Text::class.java

            FileInputFormat.addInputPath(job, input)
            FileOutputFormat.setOutputPath(job, output)
            output.getFileSystem(conf).delete(output)
            return job
        }
    }
}

