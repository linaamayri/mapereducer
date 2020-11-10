package com.opstty.job;

        import com.opstty.mapper.SmallestToLargestMapper;
        import com.opstty.reducer.SmallestToLargestReducer;
        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
        import org.apache.hadoop.util.GenericOptionsParser;

public class SmallestToLargest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: job185 <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "job185");
        job.setJarByClass(SmallestToLargest.class);
        job.setMapperClass(SmallestToLargestMapper.class);
        job.setCombinerClass(SmallestToLargestReducer.class);
        job.setReducerClass(SmallestToLargestReducer.class);
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