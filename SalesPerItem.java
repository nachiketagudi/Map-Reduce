package com.nachiketa.mapreduce.example;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class SalesPerItem {

	
	public static class E_EMapper extends MapReduceBase implements 
	   Mapper<LongWritable ,/*Input key Type */ 
	   Text,                /*Input value Type*/ 
	   Text,                /*Output key Type*/ 
	   IntWritable>        /*Output value Type*/ 
	
	   {

		@Override
		public void map(LongWritable key, Text value,OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line,",");
			String fruit=tokenizer.nextToken();
			int amount=0;
			int quantity=0;
			while(tokenizer.hasMoreElements()){
				quantity=Integer.parseInt(tokenizer.nextToken());
				amount=Integer.parseInt(tokenizer.nextToken());
			}
			output.collect(new Text(fruit), new IntWritable(amount*quantity)); 

		} 
	   }
	
	public static class E_EReduce extends MapReduceBase implements 
	Reducer< Text, IntWritable, Text, IntWritable >  
	   {

		@Override
		public void reduce(Text key, Iterator<IntWritable> value,OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum=0;
			int count=0;
			while(value.hasNext()){
				count=count+1;
				sum=sum+Integer.parseInt(value.next().toString());
			}
			
			 output.collect(key, new IntWritable(sum)); 
		}

	
	   
	   }
	
	
	public static void main(String[] args) {
		JobConf conf = new JobConf(SalesPerItem.class); 
	      
	      conf.setJobName("SalesPerItem_mapredjob"); 
	      conf.setOutputKeyClass(Text.class);
	      conf.setOutputValueClass(IntWritable.class); 
	      conf.setMapperClass(E_EMapper.class); 
	      conf.setCombinerClass(E_EReduce.class); 
	      conf.setReducerClass(E_EReduce.class); 
	      conf.setInputFormat(TextInputFormat.class); 
	      conf.setOutputFormat(TextOutputFormat.class); 
	      
	      FileInputFormat.setInputPaths(conf, new Path(args[0])); 
	      FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
	      
	      try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			e.printStackTrace();
		} 

	}

}
