package org.robert.max;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderComparator extends WritableComparator{

    public OrderComparator(){
        super(Order.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Order order1=(Order)a;
        Order order2=(Order)b;
        return order1.getItemid().compareTo(order2.getItemid());
    }

}
