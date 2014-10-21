package wordCount;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
//设置input ，output 序列化文件
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.io.StringReader;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import wordCount.Html.HtmlCombiner;
import wordCount.Html.HtmlMapper;
import wordCount.Html.HtmlReducer;

public class kmeans {
	

   public static class KMapper1 
      extends Mapper<Text, Text, Text, Text>{
   private double[][] centers;
   private int dimention_m ;
   private int dimention_n ;
   public void setup(Context context)throws IOException, InterruptedException
   		{
	    	Text key = new Text();
	    	Text val = new Text();
			Path path = new Path(context.getConfiguration().get("path"));
			FileSystem fs = path.getFileSystem(context.getConfiguration());
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, context.getConfiguration());
	   		//System.out.printf("%s\t", cache[0].toString());
	   		List<ArrayList<Double>> temp_centers = new ArrayList<ArrayList<Double>>();
	   		ArrayList<Double> center;
	   		String line;
	   		
	   		while(reader.next(key,val) != false)
	   		{
	   			line = val.toString();
	   			center = new ArrayList<Double>();
	   			String[] str = line.split(",");
	   			for(int i = 0; i<str.length;i++)
	   			{
	   				center.add(Double.parseDouble(str[i]));
	   			}
	   			temp_centers.add(center);
	   		}
	   		//System.out.printf("%s\t", "run");
	   		reader.close();
	   		ArrayList<Double>[]  newcenters = temp_centers.toArray(new ArrayList[]{});
	   		dimention_m =temp_centers.size();
	   		dimention_n =newcenters[0].size();
	   		centers= new double[dimention_m][dimention_n];
	   		for (int i = 0;i<dimention_m;i++)
	   		{
	   			Double[] temp_double = newcenters[i].toArray(new Double[]{});
	   			for(int j=0; j<dimention_n;j++)
	   			{
	   				centers[i][j] = temp_double[j];
	   			}
	   		}
	   		//System.out.printf("%s\t", "run");
   		}
   public void map(Text key, Text value, Context context
                   ) throws IOException, InterruptedException {
	   String[] values = value.toString().split(",");
	   
	   if(values.length != dimention_n)
	   {
		   return;
	   }
	   double[] temp_double = new double[values.length];
	   for(int i= 0;i<values.length;i++)
	   {
		   temp_double[i] = Double.parseDouble(values[i]);
	   }
	   double distance = Double.MIN_VALUE;
	   double temp_distance = 0.0;
	   double tempcenter = 0.0;
	   double tempelement = 0.0;
	   double result = 0.0;
	   double tempsqrt=0.0;
	   int index = 0;
	   for(int i= 0;i<dimention_m;i++)
	   {
		   
		   double[] temp_center = centers[i];
		   temp_distance = 0.0;
		   tempcenter = 0.0;
		   tempelement = 0.0;
		   tempsqrt=0.0;
		   for(int j = 0; j<dimention_n;j++)
		   {
			   temp_distance = temp_distance+Math.abs((temp_center[j]*temp_double[j]));
			   tempcenter = tempcenter + temp_center[j]*temp_center[j];
			   tempelement = tempelement+temp_double[j]*temp_double[j];
		   }
		   tempsqrt =Math.sqrt(tempcenter)*Math.sqrt(tempelement);
		   if(tempsqrt > Double.MIN_VALUE)
		   result = temp_distance/tempsqrt;
		   else
		   {
			   tempsqrt = Double.MIN_VALUE;
			   result =temp_distance/tempsqrt;;
		   }
		   if(result > distance)
		   {
			   index = i;
			   distance = result;
		   }
	   }
	   //System.out.printf("%s  %s\t", key,index);
	   context.write(new Text(String.valueOf(index)),value);
   }
 }
 
 public static class KReducer1 
      extends Reducer<Text,Text,Text,Text> {
	   private int dimention ;
	   public void setup(Context context)throws IOException, InterruptedException
  		{

	    	Text key = new Text();
	    	Text val = new Text();
	   		Path path = new Path(context.getConfiguration().get("path"));
			FileSystem fs = path.getFileSystem(context.getConfiguration());
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, context.getConfiguration());
	   		List<ArrayList<Double>> temp_centers = new ArrayList<ArrayList<Double>>();
	   		ArrayList<Double> center;


	
			   

	   		String line;
	   		while(reader.next(key,val)!=false)
	   		{
	   			line = val.toString();
	   			String[] str = line.split(",");
	   			dimention = str.length;
	   			reader.next(key,val);
	   			break;
	   		}
	   		reader.close();
  		}
   public void reduce(Text key, Iterable<Text> values, 
                      Context context
                      ) throws IOException, InterruptedException {
	  
	   double[] sum = new double[dimention];
	   int sumCount = 0;
	   for(Text val :values)
	   {
		   String[] datastr = val.toString().split(",");
		   for(int i = 0;i<dimention;i++)
		   {
			   sum[i] += Double.parseDouble(datastr[i]);
		   }
		   sumCount ++;
	   }
	   StringBuffer sb = new StringBuffer();
	   for(int i= 0;i<dimention;i++)
	   {
		   sb.append(sum[i]/sumCount + ",");
	   }
	   context.write(key, new Text(sb.toString()));
	   /**
	   for(Text val:values)
	   {
		   context.write(key, val);
	   }
	   **/
   }
 }
 
 public static class KMapper 
 extends Mapper<Text, Text, LongWritable,IntWritable>{
private double[][] centers;
private int dimention_m ;
private int dimention_n ;
public void setup(Context context)throws IOException, InterruptedException
		{
   	Text key = new Text();
   	Text val = new Text();
	Path path = new Path(context.getConfiguration().get("path"));
	FileSystem fs = path.getFileSystem(context.getConfiguration());
	SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, context.getConfiguration());
  		//System.out.printf("%s\t", cache[0].toString());
  		List<ArrayList<Double>> temp_centers = new ArrayList<ArrayList<Double>>();
  		ArrayList<Double> center;
  		String line;
  		
  		while(reader.next(key,val) != false)
  		{
  			line = val.toString();
  			center = new ArrayList<Double>();
  			String[] str = line.split(",");
  			for(int i = 0; i<str.length;i++)
  			{
  				center.add(Double.parseDouble(str[i]));
  			}
  			temp_centers.add(center);
  		}
  		//System.out.printf("%s\t", "run");
  		reader.close();
  		ArrayList<Double>[]  newcenters = temp_centers.toArray(new ArrayList[]{});
  		dimention_m =temp_centers.size();
  		dimention_n =newcenters[0].size();
  		centers= new double[dimention_m][dimention_n];
  		for (int i = 0;i<dimention_m;i++)
  		{
  			Double[] temp_double = newcenters[i].toArray(new Double[]{});
  			for(int j=0; j<dimention_n;j++)
  			{
  				centers[i][j] = temp_double[j];
  			}
  		}
  		//System.out.printf("%s\t", "run");
		}
public void map(Text key, Text value, Context context
              ) throws IOException, InterruptedException {
  String[] values = value.toString().split(",");
  
  if(values.length != dimention_n)
  {
	   return;
  }
  double[] temp_double = new double[values.length];
  for(int i= 0;i<values.length;i++)
  {
	   temp_double[i] = Double.parseDouble(values[i]);
  }
  double distance = Double.MIN_VALUE;
  double temp_distance = 0.0;
  double tempcenter = 0.0;
  double tempelement = 0.0;
  double result = 0.0;
  double tempsqrt=0.0;
  int index = 0;
  for(int i= 0;i<dimention_m;i++)
  {
	   
	   double[] temp_center = centers[i];
	   temp_distance = 0.0;
	   tempcenter = 0.0;
	   tempelement = 0.0;
	   tempsqrt=0.0;
	   for(int j = 0; j<dimention_n;j++)
	   {
		   temp_distance = temp_distance+Math.abs((temp_center[j]*temp_double[j]));
		   tempcenter = tempcenter + temp_center[j]*temp_center[j];
		   tempelement = tempelement+temp_double[j]*temp_double[j];
	   }
	   tempsqrt =Math.sqrt(tempcenter)*Math.sqrt(tempelement);
	   if(tempsqrt > Double.MIN_VALUE)
	   result = temp_distance/tempsqrt;
	   else
	   {
		   tempsqrt = Double.MIN_VALUE;
		   result =temp_distance/tempsqrt;;
	   }
	   if(result > distance)
	   {
		   index = i;
		   distance = result;
	   }
  }
  //System.out.printf("%s\t", index);
  IntWritable val = new IntWritable(index);
  LongWritable k = new LongWritable(Long.valueOf(key.toString()));
  context.write(k, val);
  //context.write(key,new Text(String.valueOf(index)));
}
}

public static class KReducer 
 extends Reducer<LongWritable,IntWritable,LongWritable,IntWritable> {
  
public void reduce(LongWritable key, IntWritable value, 
                 Context context
                 ) throws IOException, InterruptedException {
 
		
		context.write(key,value);

}
}
private int determinestop(int nk,Configuration conf,Path recent,Path old) throws Exception
{
    	FileSystem fs = recent.getFileSystem(conf);
    	SequenceFile.Reader reader = new SequenceFile.Reader(fs, recent, conf);
		List<ArrayList<Double>> temp_centers = new ArrayList<ArrayList<Double>>();
		ArrayList<Double> center;
		String line;
		Text key = new Text();
		Text val = new Text();
		while(reader.next(key,val) != false)
		{
			line = val.toString();
			center = new ArrayList<Double>();
			String[] str = line.split(",");
			for(int i = 0; i<str.length;i++)
			{
				center.add(Double.parseDouble(str[i]));
			}
			temp_centers.add(center);
		}
		//System.out.printf("%s\t", "run");
		
		ArrayList<Double>[]  newcenters = temp_centers.toArray(new ArrayList[]{});
		int dimention_m =temp_centers.size();
		int dimention_n =newcenters[0].size();
		double[][] centerrecent= new double[dimention_m][dimention_n];
		for (int i = 0;i<dimention_m;i++)
		{
			Double[] temp_double = newcenters[i].toArray(new Double[]{});
			for(int j=0; j<dimention_n;j++)
			{
				centerrecent[i][j] = temp_double[j];
			}
		}
		reader.close();
		reader = new SequenceFile.Reader(fs, old, conf);
		temp_centers = new ArrayList<ArrayList<Double>>();
		while(reader.next(key,val) != false)
		{
			line = val.toString();
			center = new ArrayList<Double>();
			String[] str = line.split(",");
			for(int i = 0; i<str.length;i++)
			{
				center.add(Double.parseDouble(str[i]));
			}
			temp_centers.add(center);
		}
		//System.out.printf("%s\t", "run");
		newcenters = temp_centers.toArray(new ArrayList[]{});
		dimention_m =temp_centers.size();
		dimention_n =newcenters[0].size();
		double[][] centerold= new double[dimention_m][dimention_n];
		for (int i = 0;i<dimention_m;i++)
		{
			Double[] temp_double = newcenters[i].toArray(new Double[]{});
			for(int j=0; j<dimention_n;j++)
			{
				centerold[i][j] = temp_double[j];
			}
		}
		reader.close();
		

		double tmp = 0.0;
		double total = 0.0;
		double total2=0.0;
		int n = 0;
		for (int i = 0;i<dimention_m;i++)
		{
			for(int j=0; j<dimention_n;j++)
			{
				tmp += Math.abs(centerrecent[i][j]-centerold[i][j]);
				total += centerold[i][j];
				total2 +=centerrecent[i][j];
			}
			if(tmp < dimention_n * Double.MIN_VALUE)
			{
				//(tmp/total) < 0.003
				//System.out.println(tmp);
				//System.out.println(total);
				//System.out.println(total2);
				//System.out.println(tmp/total);
				//return 1;
				n++;
			}
			tmp =0;
			total=0;
			total2 = 0;
		}
		if(n==nk)
			return 1;
	return 0;
}
private double[] readcenter(SequenceFile.Reader reader) throws Exception
{
	//System.out.printf("%s\t", cache[0].toString());
	List<ArrayList<Double>> temp_centers = new ArrayList<ArrayList<Double>>();
	ArrayList<Double> center;
	String line;
	Text key = new Text();
	Text val = new Text();
	while(reader.next(key,val) != false)
	{
		line = val.toString();
		center = new ArrayList<Double>();
		String[] str = line.split(",");
		for(int i = 0; i<str.length;i++)
		{
			center.add(Double.parseDouble(str[i]));
		}
		temp_centers.add(center);
	}
	//System.out.printf("%s\t", "run");
	
	ArrayList<Double>[]  newcenters = temp_centers.toArray(new ArrayList[]{});
	int dimention_m =temp_centers.size();
	int dimention_n =newcenters[0].size();
	double[] centerlist= new double[dimention_m*dimention_n];
	for (int i = 0;i<dimention_m;i++)
	{
		Double[] temp_double = newcenters[i].toArray(new Double[]{});
		for(int j=0; j<dimention_n;j++)
		{
			centerlist[i*dimention_m+j] = temp_double[j];
		}
	}
	return centerlist;
}
	public int run(String k ,String n,String input, String output,String tmp) throws Exception{
		Configuration conft = new Configuration();
		//随机读入k个中心点。
		SequenceFile.Reader reader;
		SequenceFile.Writer writer;
	    Path path = new Path(input+"/part-r-00000");
	    Path centerpath  = new Path(tmp+"center");
	    FileSystem fs = path.getFileSystem(conft);
	    reader = new SequenceFile.Reader(fs, path, conft);
	    writer = new SequenceFile.Writer(fs, conft,centerpath, Text.class, Text.class);
	    Text key = new Text();
	    Text val = new Text();
	    int nk = Integer.parseInt(k);
	    //FileSystem hdfs = centerpath.getFileSystem(conft);
	   // hdfs.create(centerpath, true);

	    for(int i = 0;i<nk;i++)
	    {
	    reader.next(key,val);
	    writer.append(key, val);//new Text(String.valueOf(i))
	    }
	    reader.close();
	    writer.close();



	    int i;
	    for(i = 0; i< Integer.parseInt(n);i++)
	    {
			Configuration conf = new Configuration();
			if(i==0)
			{
			//DistributedCache.addCacheFile(centerpath.toUri(), conf);
			///DistributedCache.createSymlink(conf);
			//DistributedCache.addLocalFiles(conf, centerpath.toString());
			//这也许是单节点需要修改之处。
			//DistributedCache.
			conf.set("path", centerpath.toString());
			}
			else{
				//DistributedCache.addCacheFile((new Path(tmp + String.valueOf(i-1)+"/part-r-00000")).toUri(), conf);
				///DistributedCache.createSymlink(conf);
				conf.set("path", tmp + String.valueOf(i-1)+"/part-r-00000");
			}
		    FileSystem hdfs = centerpath.getFileSystem(conf);
		    //hdfs.copyFromLocalFile(true, true, centerpath, new Path(output));
	   // System.out.printf("%s\t", centerpath.toUri());

		Job job1 = new Job(conf, "kmeans");
	    job1.setJarByClass(kmeans.class);
	    job1.setMapperClass(KMapper1.class);
	    job1.setCombinerClass(KReducer1.class);
	    job1.setReducerClass(KReducer1.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);
	    job1.setInputFormatClass(SequenceFileInputFormat.class);
	    job1.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileInputFormat.addInputPath(job1, new Path(input));
	    FileOutputFormat.setOutputPath(job1, new Path(tmp + String.valueOf(i)));
	    job1.waitForCompletion(true);
	    //之后删除，节省时间
	    if(i >= 2)
	    {
	    	///加入中心判定。
	    	if(determinestop(nk,conf,new Path(tmp + String.valueOf(i)+"/part-r-00000"),new Path(tmp + String.valueOf(i-1)+"/part-r-00000")) == 1)
	    		break;
	    	
	    	hdfs.delete(new Path(tmp + String.valueOf(i-1)), true);
	    	
	    	
	    }
	    if((i == Integer.parseInt(n)-1) && (i >= 1))
		{
	    	hdfs.delete(new Path(tmp + String.valueOf(0)), true);
		}
		//这也许是单节点需要修改之处。
		//DistributedCache.
	    
		/*
	    
	    FileSystem hdfs = centerpath.getFileSystem(conf);
	    hdfs.copyFromLocalFile(true, true, centerpath, new Path(output));
	    hdfs.delete(centerpath, true);
	    */
	    
	    }
	    
	    Configuration conf = new Configuration();
		//DistributedCache.addCacheFile((new Path(tmp + String.valueOf(i-1)+"/part-r-00000")).toUri(), conf);
		//DistributedCache.addLocalFiles(conf, tmp + String.valueOf(i-1)+"/part-r-00000");
	    conf.set("path", tmp + String.valueOf(i-1)+"/part-r-00000");
	    
		Job job = new Job(conf, "kmeans");
	    job.setJarByClass(kmeans.class);
	    job.setMapperClass(KMapper.class);
	    job.setCombinerClass(KReducer.class);
	    job.setReducerClass(KReducer.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.waitForCompletion(true);
	    return 0;
	}
}
