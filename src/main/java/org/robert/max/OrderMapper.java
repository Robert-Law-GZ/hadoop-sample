package org.robert.max;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable,Text,Order,NullWritable>{
    private Order order=new Order();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] fields=line.split(",");
        order.setItemid(new Text(fields[0]));
        order.setAmount(new DoubleWritable(Double.parseDouble(fields[2])));
        context.write(order,NullWritable.get());
    }

}
