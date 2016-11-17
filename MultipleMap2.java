package ru.vsu.amm.at;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MultipleMap2 extends Mapper<IntWritable, Text, Text, Text>
{

    Text KEY = new Text();
    Text VALUE = new Text();


    public void map(Text KEY, Text value, Context context) throws IOException, InterruptedException
    {
        String line=value.toString();
        String[] words=line.split(" ");
        KEY.set(words[0]);
        VALUE.set(words[1]);
        context.write(KEY, VALUE);
    }
}
