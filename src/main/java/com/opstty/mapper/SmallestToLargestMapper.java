package com.opstty.mapper;

        import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Mapper;

        import java.io.IOException;
        import java.util.StringTokenizer;

public class SmallestToLargestMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final Text values = new Text();


    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");


        while (itr.hasMoreTokens()){
            values.set(itr.nextToken());
            if(values.toString().contains(";")) {
                Text arrNum = new Text(values.toString().split(";")[3]); //recupere l'arbre
                String valNum = values.toString().split(";")[6]; //recupere la taille
                if(!valNum.equals("")){ //empty line
                    if (!arrNum.equals(new Text("ESPECE"))) {
                        context.write(arrNum, new IntWritable((int)Double.parseDouble(valNum)));
                        //System.out.println(arrNum.toString());
                    }
                }
            }
        }
    }
}
