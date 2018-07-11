package org.robert.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sum_upFlow=0;
        long sum_dFlow=0;

        for (FlowBean bean:values){
            sum_upFlow+=bean.getUpFlow();
            sum_dFlow+=bean.getdFlow();
        }

        FlowBean resultBean=new FlowBean(sum_upFlow,sum_dFlow);
        context.write(key,resultBean);
    }

}
