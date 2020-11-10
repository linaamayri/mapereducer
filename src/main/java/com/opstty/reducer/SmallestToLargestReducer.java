package com.opstty.reducer;

        import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Reducer;

        import java.io.IOException;
        import java.util.*;

public class SmallestToLargestReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    TreeMap<Text,IntWritable> result=new TreeMap<Text, IntWritable>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int maxVal = 0;
        for (IntWritable val : values) {
            if (val.get() > maxVal) {
                maxVal = val.get();
            }

        }
        result.put(new Text(key), new IntWritable(maxVal));
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        Set<Map.Entry<Text, IntWritable>> set = result.entrySet();
        List<Map.Entry<Text, IntWritable>> list = new ArrayList<Map.Entry<Text,IntWritable>>(set);
        Collections.sort( list, new Comparator<Map.Entry<Text, IntWritable>>()
        {
            public int compare( Map.Entry<Text, IntWritable> o1, Map.Entry<Text,IntWritable> o2 )
            {
                return (o1.getValue()).compareTo( o2.getValue() );
            }
        });
        for(Map.Entry<Text,IntWritable> entry:list){
            context.write(entry.getKey(),entry.getValue());
        }

    }
}