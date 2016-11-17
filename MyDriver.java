package ru.vsu.amm.at;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class MyDriver{

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
        Path p1 = new Path(files[0]);
        Path p2 = new Path(files[1]);
        Path p3 = new Path(files[2]);


        if (fs.exists(p3)) {
            fs.delete(p3, true);
        }

        Job job = new Job(conf, "wordcount");
        job.setJarByClass(MyDriver.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);


        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        MultipleInputs.addInputPath(job, p1, TextInputFormat.class, MultipleMap1.class);
        MultipleInputs.addInputPath(job, p2, TextInputFormat.class, MultipleMap2.class);


        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());

        job.waitForCompletion(true);
    }
}