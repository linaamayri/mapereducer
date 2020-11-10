package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DistrictContainingTreesMapper extends Mapper<Object, Text, Text, NullWritable> {
   // private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text word_bis = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            if (value.toString().contains("GEOPOINT") /*Some condition satisfying it is header*/)
                return;
            else {
                StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
                while (itr.hasMoreTokens()) {

                    word.set(itr.nextToken().split(";")[1]);
                    context.write(word, NullWritable.get());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}