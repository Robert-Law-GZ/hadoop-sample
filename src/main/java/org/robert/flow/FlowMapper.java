package org.robert.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();

        System.out.println("--:"+line);

        String[] fields=line.split("\t");
        String phoneNbr=fields[0];
        long upFlow=Long.parseLong(fields[1]);
        long dFlow=Long.parseLong(fields[2]);
        context.write(new Text(phoneNbr),new FlowBean(upFlow,dFlow));
    }

}
