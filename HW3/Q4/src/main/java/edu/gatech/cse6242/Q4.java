package edu.gatech.cse6242;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.w3c.dom.Text;

import java.io.IOException;

public class Q4 {
	
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job1 = Job.getInstance(conf, "Job1");

    /* TODO: Needs to be implemented */
    job1.setJarByClass(Q4.class);
    job1.setMapperClass(StatDegMapper.class);
    job1.setCombinerClass(DegReducer.class);
    job1.setReducerClass(DegReducer.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path("temp"));

    job1.waitForCompletion(true);
    Job job2 = Job.getInstance(conf, "Job2");
    job2.setJarByClass(Q4.class);
    job2.setMapperClass(DegDiffMapper.class);
    job2.setCombinerClass(DegReducer.class);
    job2.setReducerClass(DegReducer.class);
    
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(IntWritable.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path("temp"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0:1);
  }

  public static class StatDegMapper extends Mapper<Object, Text, Text, IntWritable>{
	  private IntWritable s = new IntWritable(1);
	  private IntWritable t = new IntWritable(-1);
	  protected void map(Object key, Text value, Context context) 
	    throws IOException, InterruptedException{
		  String[] split = value.toString().split("\t");
		  if(split.length == 2){
			  context.write(new Text(split[0]), s);
			  context.write(new Text(split[1]), t);
		  }
	  }
  }
 
  public static class DegDiffMapper extends Mapper<Object, Text, Text, IntWritable>{
	  private IntWritable one = new IntWritable(1);
	  protected void map(Object key, Text value, Context context) 
	    throws IOException, InterruptedException{	  
			  String[] split = value.toString().split("\t");
			  if(split.length == 2){
				  context.write(new Text(split[1]), one);
		  }
	  }
  }
  
  public static class DegReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	  private IntWritable result = new IntWritable();
	  protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
	    throws IOException, InterruptedException{
		  int sum = 0;
		  for (IntWritable val : values) {
			  sum += val.get();
		  }
		  result.set(sum);
		  context.write(key, result);
		  }
	  }
  
}
