package ru.technodiasoft.exaple.counter;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;


import java.io.IOException;

/**
 * Created by gevorg.hachaturyan on 17/05/2017.
 */
public class Counter extends Configured implements Tool {

    public static void main(String... args) {

    }

    public int run(String... args) throws IOException, InterruptedException, ClassNotFoundException {

        if (args.length != 2) {
            System.err.printf("Needs two arguments <input> <output> \n",
                    getClass().getSimpleName());
            return -1;
        }

        Job job = new Job();
        job.setJarByClass(Counter.class);
        job.setJobName("Word counter");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        int returnValue = job.waitForCompletion(true) ? 0 : 1;

        if (job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if (!job.isSuccessful()) {
            System.out.println("Job was not successful");
        }

        return returnValue;
    }
}
