package org.robert.max;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Order implements WritableComparable<Order>{
    private Text itemid;
    private DoubleWritable amount;

    public Order(){

    }

    public Order(Text id,DoubleWritable amount){
        this.itemid=id;
        this.amount=amount;
    }

    public Text getItemid() {
        return itemid;
    }

    public void setItemid(Text itemid) {
        this.itemid = itemid;
    }

    public DoubleWritable getAmount() {
        return amount;
    }

    public void setAmount(DoubleWritable amount) {
        this.amount = amount;
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.itemid=new Text(dataInput.readUTF());
        this.amount=new DoubleWritable(dataInput.readDouble());
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(itemid.toString());
        dataOutput.writeDouble(amount.get());
    }

    public int compareTo(Order o) {
        int ret=this.itemid.compareTo(o.getItemid());

        if (ret==0){
            ret=-this.amount.compareTo(o.getAmount());
        }

        return ret;
    }

    @Override
    public String toString() {
        return itemid.toString()+":"+"\t"+amount.get();
    }

}
