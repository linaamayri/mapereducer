package com.opstty.job;

import com.opstty.mapper.DistrictContainingTreesMapper;
import com.opstty.mapper.NbrTreesSpeciesMapper;
import com.opstty.reducer.DistrictContainingTreesReducer;
import com.opstty.reducer.NbrTreesSpeciesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NbrTreesSpecies {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: nbr_trees_species <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "nbr_trees_species");
        job.setJarByClass(NbrTreesSpecies.class);
        job.setMapperClass(NbrTreesSpeciesMapper.class);
        job.setCombinerClass(NbrTreesSpeciesReducer.class);
        job.setReducerClass(NbrTreesSpeciesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

