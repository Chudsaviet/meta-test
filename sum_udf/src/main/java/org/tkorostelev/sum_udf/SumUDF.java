package org.tkorostelev.sum_udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;

public class SumUDF extends UDF {
  
    public IntWritable evaluate(IntWritable x, IntWritable y) {

        return new IntWritable(x.get() + y.get());
    }

}