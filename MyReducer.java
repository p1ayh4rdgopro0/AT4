package ru.vsu.amm.at;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    public String KEY = "";
    Text VALUE = new Text();


    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for (IntWritable value : values) {
            if (i == 0) {
                KEY = value.toString() + ",";
            } else {
                KEY += value.toString();
            }
            i++;
        }
        VALUE.set(KEY);
        context.write(key, VALUE);

    }
}
