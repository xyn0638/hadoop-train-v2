package com.xuyanan.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AccessReducer extends Reducer<Text, Access, NullWritable, Access> {
    @Override
    protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {
        long up = 0;
        long down = 0;

        for(Access access: values) {
            up += access.getUp();
            down += access.getDown();
        }

        context.write(NullWritable.get(), new Access(key.toString(), up, down));
    }
}
