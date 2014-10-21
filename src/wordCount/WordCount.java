package wordCount;
import wordCount.Html;
import wordCount.Html.HtmlMapper;
import wordCount.Html.HtmlReducer;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
  
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {


    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {

    }
  }

  public static void main(String[] args) throws Exception {
    
	Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 5) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    
    //Path centerpath  = new Path(otherArgs[3]+"center");
   // FileSystem hdfs = centerpath.getFileSystem(conf);
   // hdfs.create(centerpath, true);
    
    Html html = new Html();
    //System.out.printf("%s\t", otherArgs[2]);
    html.run(otherArgs[0],otherArgs[2]+"/");
    
    kmeans kmeans = new kmeans();
    kmeans.run(otherArgs[3],otherArgs[4],otherArgs[2]+"/TFIDFC", otherArgs[1],otherArgs[2]+"/");
    
    /**
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    **/
    //FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
    //FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

    
    
    
  }
}

