package org.robert.max;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.robert.flow.FlowBean;
import org.robert.flow.FlowMapper;
import org.robert.flow.FlowPartitionMain;
import org.robert.flow.FlowReducer;

import java.io.IOException;

public class OrderMain {

    public static void main(String[] args) throws IOException {
        Configuration conf=new Configuration();
        Job job= Job.getInstance(conf,"flow count");

        job.setJarByClass(OrderMain.class);

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Order.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Order.class);
        job.setPartitionerClass(OrderPartitioner.class);
        job.setNumReduceTasks(1);
        job.setGroupingComparatorClass(OrderComparator.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        try {
            boolean result = job.waitForCompletion(true);
            System.exit(result?0:1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

}
