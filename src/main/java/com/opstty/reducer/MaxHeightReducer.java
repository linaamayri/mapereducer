package com.opstty.reducer;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxHeightReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int maxheight = 0;
        for (IntWritable val : values) {
            if (val.get() > maxheight) {
                maxheight = val.get();
            }
        }
        context.write(key, new IntWritable(maxheight));
    }
}

