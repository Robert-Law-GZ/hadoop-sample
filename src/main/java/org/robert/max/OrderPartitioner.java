package org.robert.max;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<Order,NullWritable>{
    @Override
    public int getPartition(Order order, NullWritable nullWritable, int i) {
        return (order.getItemid().hashCode()&Integer.MAX_VALUE)%i;
    }
}
