package com.opstty.job;

import com.opstty.mapper.DistrictContainingTreesMapper;
import com.opstty.reducer.DistrictContainingTreesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DistrictContainingTrees {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: district_containing_trees <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "district_containing_trees");
        //job.setNumReduceTasks(0);
        job.setJarByClass(DistrictContainingTrees.class);
        job.setMapperClass(DistrictContainingTreesMapper.class);
        job.setCombinerClass(DistrictContainingTreesReducer.class);
        job.setReducerClass(DistrictContainingTreesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
