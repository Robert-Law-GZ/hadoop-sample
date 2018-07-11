package org.robert.flow;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;


public class FlowPartition extends Partitioner<Text,FlowBean>{

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String prefix=text.toString().substring(0,3);

        if (prefix.equalsIgnoreCase("138")){
            return 0;
        }

        return 1;
    }

}
