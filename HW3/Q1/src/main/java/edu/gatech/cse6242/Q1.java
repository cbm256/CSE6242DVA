package edu.gatech.cse6242;
import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1 {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q1");

    /* TODO: Needs to be implemented */
    job.setJarByClass(Q1.class);
    job.setMapperClass(WeightMap.class);
    job.setCombinerClass(WeightReduce.class);
    job.setReducerClass(WeightReduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  
  public static class WeightMap extends Mapper<Object, Text, Text, IntWritable>{
	  private IntWritable weightflow = new IntWritable();
	  protected void map(Object key, Text value, Context context)
	  	throws IOException, InterruptedException{
		  String[] split = value.toString().split("\t");
		  String target = split[1];
		  String weight = split[2];
		  weightflow = new IntWritable(Integer.parseInt(weight));
		  context.write(new Text(target), weightflow);
	  }
  } 
  public static class WeightReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
	  private IntWritable result = new IntWritable();
	  protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
	    throws IOException, InterruptedException{
		 int min = Integer.MAX_VALUE;
		 for(IntWritable val: values){
			 if(val.get() < min){
				 min = val.get();
			 }
		 }
		 result.set(min);
		 context.write(key, result);
	  }
  }
	  
  
}
