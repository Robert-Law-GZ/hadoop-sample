package org.robert;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text word=new Text();
    private static final IntWritable one = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString().replaceAll("<"," ").replaceAll(">"," ").replaceAll("-"," ");

        String[] words=line.split(" ");

        for (String word:words){
            context.write(new Text(word),one);
        }

    }

}