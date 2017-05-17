package ru.technodiasoft.exaple.counter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by gevorg.hachaturyan on 17/05/2017.
 */
public class MapClass  extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable ONE = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value,
                       Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        StringTokenizer st = new StringTokenizer(line," ");

        while(st.hasMoreTokens()){
            word.set(st.nextToken());
            context.write(word,ONE);
        }

    }
}
