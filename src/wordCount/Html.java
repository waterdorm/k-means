package wordCount;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;

public class Html{
	
	  //private static SequenceFile.Reader reader = null;
	  
	  public static class HtmlMapper extends Mapper<LongWritable, Text,Text, Text>
      {
		  private final static IntWritable one = new IntWritable(1);
		  private static Text words = new Text();
		  private Text filekey = new Text();
   public void map(LongWritable key, Text value, Context context
                   ) throws IOException, InterruptedException {
	   filekey = new Text(key.toString());
	   String word = value.toString();
	   Element d= Jsoup.parse(word).body();
	   if(d == null)
	   {
		   return;
	   }
	   String regex = "[\u4e00-\u9fa5]+";
	   StringBuffer buf = new StringBuffer();
	   Matcher matcher = Pattern.compile(regex).matcher(d.toString());
	   if(matcher == null)
	   {
		   return ;
	   }
	   int start = 0;
	   while(matcher.find(start))
	   {
		  start = matcher.end()+1;
		  //用来累加所有的单词数
		  buf.append(matcher.group());
	   }
	   if(buf.length() == 0)
	   {
		 // buf.append("TFIDF");
	   }
	   List<Term> Terms = ToAnalysis.paser(buf.toString());
       Iterator<Term> term = Terms.iterator();
       while (term.hasNext()) 
       {
       	words.set( term.next().toString() + ","+key.toString());
           context.write(words, new Text(one.toString()));
       }
	   
	   }

   }
public static class HtmlCombiner
	  extends Reducer<Text, Text, Text, Text> {
   public void reduce(Text key, Iterable <Text> values, 
                      Context context
                      ) throws IOException, InterruptedException
       {

	   		long sum = 0;
	   		for(Text val :values)
	   		{
	   			sum = sum +Long.parseLong(val.toString());
	   		}
	   		//单词统计次数完成。
	   		context.write(new Text(key.toString()), new Text(String.valueOf(sum)));
	   
       }
	   
  }
public static class HtmlReducer 
	  extends Reducer<Text, Text, Text, Text> {
   public void reduce(Text key, Iterable <Text> values, 
                      Context context
                      ) throws IOException, InterruptedException {
	   for (Text val : values)
	   {
		   context.write(key, val);
	   }
   }
}
//标准做法
public static class HtmlMapper2 extends Mapper<Text, Text,Text, Text>
{

public void map(Text key, Text value, Context context
           ) throws IOException, InterruptedException {

	String keystr = key.toString();
	int index = keystr.indexOf(",");
	String keynew = keystr.substring(index +1);
	String word = keystr.substring(0, index);
	String values = word + ","+keynew+","+value.toString();
	context.write(new Text(keynew),new Text(values));
}


}

public static class HtmlReducer2 
extends Reducer<Text, Text, Text, Text> {
	int wordnum = 0;
public void reduce(Text key, Iterable<Text> values, 
              Context context
              ) throws IOException, InterruptedException {
	Vector<String> vals = new Vector<String>();
	wordnum = 0;
	for(Text val : values)
	{
		String value = val.toString();
		vals.add(value);
		
		int index = value.lastIndexOf(",");
		String substr = value.substring(index+1);
		int num =  Integer.valueOf(substr).intValue();
		wordnum +=num;
		}
	for(int j = 0; j<vals.size();j++)
	{
		//context.write(new Text(vals.get(j)), new Text(String.valueOf(wordnum)));
		String value = vals.get(j);
		//context.write(new Text(value), new Text(String.valueOf(wordnum)));
		int index = value.lastIndexOf(",");
		String newkey = value.substring(0, index);
		String n = value.substring(index +1);
		context.write(new Text(newkey), new Text(n +","+ String.valueOf(wordnum)));
	}
}
}

public static class HtmlMapper3 extends Mapper<Text, Text,Text, Text>
{
	//private final static IntWritable one = new IntWritable(1);
public void map(Text key, Text value, Context context
           ) throws IOException, InterruptedException {
	String keystr = key.toString();
	int index = keystr.indexOf(",");
	String word = keystr.substring(0, index);
	String docname = keystr.substring(index + 1);
	String values = value.toString();
	index = values.indexOf(",");
	String n = values.substring(0, index);
	String N = values.substring(index +1);
	context.write(new Text(word),new Text(docname+","+n+","+N));

	}
}

public static class HtmlReducer3 
extends Reducer<Text, Text, Text, Text> {
int wordnum = 0;
public void reduce(Text key, Iterable<Text> values, 
              Context context
              ) throws IOException, InterruptedException {
		
		long sum = 0;
		Vector<String> vals = new Vector<String>();
		for(Text val :values)
		{
			sum = sum + 1;
			vals.add(val.toString());
			//context.write(key,val);
		}
		
		//单词统计次数完成。
		for(int j = 0; j<vals.size();j++)
		{
			//context.write(new Text(vals.get(j)), new Text(String.valueOf(wordnum)));
			String value = vals.get(j);
			//context.write(new Text(value), new Text(String.valueOf(wordnum)));
			int index = value.indexOf(",");
			String docname = value.substring(0, index);
			String nN = value.substring(index +1);
			context.write(new Text(key.toString()+","+docname), new Text(nN+","+String.valueOf(sum)));
		}
		
	/**
    for (Text val : values) {
        context.write(key,val);
      }
	**/
	}
}
public static class HtmlMapper4 extends Mapper<Text, Text,Text, Text>
{
	private double D = 1;
public void map(Text key, Text value, Context context
           ) throws IOException, InterruptedException {
		double TF = 0;
		double IDF = 0;
		double TFIDF = 0;
		
		String keystr = key.toString();
		int index = keystr.indexOf(",");
		String word = keystr.substring(0,index);
		String docname = keystr.substring(index +1);
		
		String valuestr = value.toString();
		index =valuestr.indexOf(",");
		String n = valuestr.substring(0, index);
		int indexN = valuestr.lastIndexOf(",");
		String N = valuestr.substring(index + 1,indexN);
		String m =valuestr.substring(indexN+1);
		
		TF = (double)(Integer.parseInt(n))/(double)(Integer.parseInt(N));
		
		double M = (double)(Integer.parseInt(m));
		if(M > D)
		{
			D = M;
		}
		if(Integer.parseInt(m)>=(int) (Math.log(D)+1) && Integer.parseInt(m) <= (D/2))
		{
			IDF = Math.log(D/(double)(Integer.parseInt(m)));
			TFIDF = TF * IDF;
	
			context.write(new Text(docname), new Text(word+","+String.valueOf(TFIDF)));
		}
		
	}
}

public static class HtmlReducer4 
extends Reducer<Text, Text, Text, Text> {
public void reduce(Text key, Iterable<Text> values, 
              Context context
              ) throws IOException, InterruptedException {
	StringBuffer all= new StringBuffer();
	for(Text val :values)
	{
		all.append(val.toString()+";");

	}
	context.write(key,new Text(all.toString()));
	}
}

public static class HtmlMapper5 extends Mapper<Text, Text,Text, Text>
{
public void map(Text key, Text value, Context context
           ) throws IOException, InterruptedException {
		Map<String,Double> map = new TreeMap<String,Double>();
		int index = 0;
		int tmp = 0;
		String val = value.toString();
		int size = val.length();
		String k = "";
		double vals = 0;
		int subindex = 0;
		String substr = "";
		while(index != size - 1)
			{
				tmp = index;
			    index = val.indexOf(";", index + 1);
			    substr = val.substring(tmp+1, index);
			    subindex = substr.indexOf(",");
			    k = substr.substring(0,subindex);
			    vals = Double.parseDouble(substr.substring(subindex+1));
				map.put(k, vals);
			}
		
		List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String,Double>>(map.entrySet());
		
		Collections.sort(list,new Comparator<Map.Entry<String,Double>>()
		{

			@Override
			public int compare(Entry<String, Double> o1,
					Entry<String, Double> o2) {
				// TODO Auto-generated method stub
				if((o1.getValue() - o2.getValue()) > 0)
						return -1;
				else
					return 1;
			}
			
		});
		StringBuffer v = new StringBuffer(":");
		String temp = "";
		int siz = list.size();
		int d = 10;
		int i;
		for(i =0;i<d;i++)
		{
			if(i>= siz)
				break;
			v.append(list.get(i).toString()+";");
		}
		for(;i<d;i++)
		{
			v.append("0=0.0;");
		}
		//此过程提取值而已
		Vector<Double> vals1 = new Vector<Double>();
		
		int index1 = 0;
		int index2 = 0;
		StringBuffer result = new StringBuffer();
		String val2 = v.toString();
		int size2 = val2.length();
		while(index2 != size2 - 1)
			{
				index1 = val2.indexOf("=",index2+1);
				index2 = val2.indexOf(";",index2+1);
				vals1.add( Double.parseDouble(val2.substring(index1 + 1, index2)));
			}
		
		for(int i1  = 0; i1 <  vals1.size();i1++)
		{
			result.append(vals1.get(i1).toString()+",");
		}

		context.write(key, new Text(result.toString()));
	}
}

public static class HtmlReducer5 extends Reducer<Text, Text, Text, Text> {
public void reduce(Text key,Text values, 
              Context context
              ) throws IOException, InterruptedException {
	/*
	Vector<Double> vals = new Vector<Double>();
	
	int index1 = 0;
	int index2 = 0;
	String value = "";
	String val = values.toString();
	int size = val.length();
	while(index2 != size - 1)
		{
			index1 = val.indexOf("=",index2+1);
			index2 = val.indexOf(";",index2+1);
			vals.add( Double.parseDouble(val.substring(index1 + 1, index2)));
		}
	
	for(int i  = 0; i <  vals.size();i++)
	{
		value = value +","+vals.get(i).toString();
	}


	*/
	context.write(key,values);

	}
}
public static class HtmlMapper6 extends Mapper<Text, Text,Text, Text>
{
public void map(Text key, Text value, Context context
           ) throws IOException, InterruptedException {

		int index = 0;
		int tmp = 0;
		String val = value.toString();
		int size = val.length();
		String k = "";
		double vals = 0;
		int subindex = 0;
		String substr = "";
		while(index != size - 1)
			{
				tmp = index;
			    index = val.indexOf(";", index + 1);
			    substr = val.substring(tmp+1, index);
			    subindex = substr.indexOf(",");
			    k = substr.substring(0,subindex);
			    vals = Double.parseDouble(substr.substring(subindex+1));
			    //也许局部出错，但key绝对不能是空的
			    if(subindex != 0)
			    {
				context.write(new Text(k), new Text(String.valueOf(vals)));
			    }
			}
	}
}

public static class HtmlReducer6 extends Reducer<Text, Text, Text, Text> {
public void reduce(Text key,Iterable<Text> values, 
              Context context
              ) throws IOException, InterruptedException {
	int n = 0;
	double tmp = 0;
	for(Text val:values)
	{
		tmp = tmp + Double.parseDouble(val.toString());
		n++;
	}
	double average = tmp/n;
	context.write(key,new Text(String.valueOf(average)));
	}
}


public static class HtmlMapper7 extends Mapper<Text, Text,Text, Text>
{
	private Vector<String> top = new Vector<String>();
	  public void setup(Context context)throws IOException, InterruptedException
  		{
			
		    //FileSystem fs = FileSystem.get(context.getConfiguration());
			   
				Text key = new Text();
				Text val = new Text();
				//Path cache[] = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				//Path newpath = new Path("file://"+cache.toString());
				Path path = new Path(context.getConfiguration().get("path"));
				FileSystem fs = path.getFileSystem(context.getConfiguration());
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, context.getConfiguration());
				//System.out.printf("%s\t", cache[0].toString());
				while(reader.next(key,val) != false)
				{
					top.add(val.toString());
				}
				//System.out.printf("%s\t", "run");
				reader.close();
			
  		}
public void map(Text key, Text value, Context context
           ) throws IOException, InterruptedException {

	int index = 0;
	int subindex = 0;
	String val = value.toString();
	String substr="";
	String result = "";
	int size = top.size();
	StringBuffer buf = new StringBuffer();
	for(int i = 0;i<size;i++)
	{
		index=val.indexOf(top.get(i)+",");
		if(index != -1)
		{
			substr = val.substring(index, val.indexOf(';', index));
			subindex =substr.indexOf(',');
			result = substr.substring(subindex+1);
			buf.append(result+",");
		}
		else
		{
			buf.append(String.valueOf(Double.MIN_VALUE)+",");
		}
	}
	//System.out.println(key);
	//System.out.println(buf);
	context.write(key, new Text(buf.toString()));
	}
}

public static class HtmlReducer7 extends Reducer<Text, Text, Text, Text> {
public void reduce(Text key,Text value, 
              Context context
              ) throws IOException, InterruptedException {

	context.write(key,value);

}
}
public int top(String input,String output,Configuration conf)throws Exception{

	SequenceFile.Reader reader = null;
	SequenceFile.Writer writer = null;
	Map<String,Double> map = new TreeMap<String,Double>();
	Path path = new Path(input+"/part-r-00000");
	Path toppath = new Path(output);
	FileSystem fs = path.getFileSystem(conf);//FileSystem.get(conf);
	
	//= FileSystem.get(conf);
	reader = new SequenceFile.Reader(fs, path, conf);
	writer = new SequenceFile.Writer(fs, conf,toppath, Text.class, Text.class);
	String k="";
	double value = 0;
	double allvalue = 0;
	double num = 0;
	Text key = new Text();
	Text val = new Text();
	while(reader.next(key,val))
	{
		k = key.toString();
		value = Double.valueOf(val.toString());
		allvalue += value;
		num +=1;
		map.put(k, value);
	}
	reader.close();

	double average = allvalue/num;
	int n = 500*((int) Math.floor(Math.log10(num)));
	List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String,Double>>(map.entrySet());
	
	Collections.sort(list,new Comparator<Map.Entry<String,Double>>()
	{

		@Override
		public int compare(Entry<String, Double> o1,
				Entry<String, Double> o2) {
			// TODO Auto-generated method stub
			if((o1.getValue() - o2.getValue()) > 0)
					return -1;
			else
				return 1;
		}
		
	});
	
	int siz = list.size();
	String str = "";
	String substr ="";
	String valuestr = "";
	int index= 0;
	int indexend =0;
	int i;
	
	for(i =0;i<n;i++)
	{
		if(i>= siz)
			break;
		
		str =list.get(i).toString();
		index = str.indexOf('=');
		substr = str.substring(0, index);
		valuestr = str.substring(index+1);
		if(Double.valueOf(valuestr) >= average)
		{
			writer.append(new Text(String.valueOf(i)),new Text(substr));
		}
	}
	
	/**
	for(i =0;i<siz;i++)
	{

		str =list.get(i).toString();
		index = str.indexOf('=');
		substr = str.substring(0, index);
		//indexend = str.indexOf(';');
		valuestr = str.substring(index+1);
		if(Double.valueOf(valuestr) >= average)
		{
			writer.append(new Text(String.valueOf(i)),new Text(substr));
		}
	}
	**/
	writer.close();
	return 0;
	}

	public int run(String input, String output) throws Exception{
		Configuration conf = new Configuration();
		
		Job job1 = new Job(conf, "Html");
	    job1.setJarByClass(Html.class);
	    job1.setMapperClass(HtmlMapper.class);
	    job1.setCombinerClass(HtmlCombiner.class);
	    job1.setReducerClass(HtmlReducer.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);
	    job1.setInputFormatClass(SequenceFileInputFormat.class);
	    job1.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileInputFormat.addInputPath(job1, new Path(input));
	    FileOutputFormat.setOutputPath(job1, new Path(output+"WF"));

	    job1.waitForCompletion(true);
	   // int D = Integer.parseInt(job1.getCounters().toString());
	    //System.out.printf("%s\t\t\t", D);
		Job job2 = new Job(conf, "Html");
	    job2.setJarByClass(Html.class);
	    job2.setMapperClass(HtmlMapper2.class);
	    //job2.setCombinerClass(HtmlReducer2.class);
	    job2.setReducerClass(HtmlReducer2.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    job2.setInputFormatClass(SequenceFileInputFormat.class);
	    job2.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileInputFormat.addInputPath(job2, new Path(output+"WF"));
	    FileOutputFormat.setOutputPath(job2, new Path(output+"WC"));

	    job2.waitForCompletion(true);

		Job job3 = new Job(conf, "Html");
	    job3.setJarByClass(Html.class);
	    job3.setMapperClass(HtmlMapper3.class);
	    //job3.setCombinerClass(HtmlReducer3.class);
	    job3.setReducerClass(HtmlReducer3.class);
	    job3.setOutputKeyClass(Text.class);
	    job3.setOutputValueClass(Text.class);
	    job3.setInputFormatClass(SequenceFileInputFormat.class);
	    job3.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileInputFormat.addInputPath(job3, new Path(output+"WC"));
	    FileOutputFormat.setOutputPath(job3, new Path(output+"WFC"));

	    job3.waitForCompletion(true);
	   
		Job job4 = new Job(conf, "Html");
	    job4.setJarByClass(Html.class);
	    job4.setMapperClass(HtmlMapper4.class);
	    //job4.setCombinerClass(HtmlReducer3.class);
	    job4.setReducerClass(HtmlReducer4.class);
	    job4.setOutputKeyClass(Text.class);
	    job4.setOutputValueClass(Text.class);
	    job4.setInputFormatClass(SequenceFileInputFormat.class);
	    job4.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileInputFormat.addInputPath(job4, new Path(output+"WFC"));
	    FileOutputFormat.setOutputPath(job4, new Path(output+"TFIDF"));
	    
	    job4.waitForCompletion(true);
	    

	    
		Job job6 = new Job(conf, "Html");
	    job6.setJarByClass(Html.class);
	    job6.setMapperClass(HtmlMapper6.class);
	    job6.setCombinerClass(HtmlReducer6.class);
	    job6.setReducerClass(HtmlReducer6.class);
	    job6.setOutputKeyClass(Text.class);
	    job6.setOutputValueClass(Text.class);
	    job6.setInputFormatClass(SequenceFileInputFormat.class);
	    job6.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileInputFormat.addInputPath(job6, new Path(output+"TFIDF"));
	    FileOutputFormat.setOutputPath(job6, new Path(output+"TFIDFF"));
	    
	    job6.waitForCompletion(true);
	    
	    top(output+"TFIDFF",output+"TOP",conf);
	    
	    conf.set("path", output+"TOP");
	    ///DistributedCache.addLocalFiles(conf, output+"TOP");
		Job job7 = new Job(conf, "Html");
	    job7.setJarByClass(Html.class);
	    job7.setMapperClass(HtmlMapper7.class);
	    //job7.setCombinerClass(HtmlReducer7.class);
	    job7.setReducerClass(HtmlReducer7.class);
	    job7.setOutputKeyClass(Text.class);
	    job7.setOutputValueClass(Text.class);
	    job7.setInputFormatClass(SequenceFileInputFormat.class);
	    job7.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileInputFormat.addInputPath(job7, new Path(output+"TFIDF"));
	    FileOutputFormat.setOutputPath(job7, new Path(output+"TFIDFC"));

	    job7.waitForCompletion(true);
	    
	    //cleanup
	    /*
	     * 
	    Job job5 = new Job(conf, "Html");
	    job5.setJarByClass(Html.class);
	    job5.setMapperClass(HtmlMapper5.class);
	    job5.setCombinerClass(HtmlReducer5.class);
	    job5.setReducerClass(HtmlReducer5.class);
	    job5.setOutputKeyClass(Text.class);
	    job5.setOutputValueClass(Text.class);
	    job5.setInputFormatClass(SequenceFileInputFormat.class);
	    job5.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileInputFormat.addInputPath(job5, new Path(output+"TFIDF"));
	    FileOutputFormat.setOutputPath(job5, new Path(output+"TFIDFC"));

	    job5.waitForCompletion(true);
	    
	    Path tmp = new Path(output + "WF");
	    FileSystem hdfs = tmp.getFileSystem(conf);
	    hdfs.delete(tmp, true);
	    tmp = new Path(output + "WC"); 
	    hdfs = tmp.getFileSystem(conf);
	    hdfs.delete(tmp, true);
	    tmp = new Path(output + "WFC"); 
	    hdfs = tmp.getFileSystem(conf);
	    hdfs.delete(tmp, true);
	    tmp = new Path(output + "TFIDF"); 
	    hdfs = tmp.getFileSystem(conf);
	    hdfs.delete(tmp, true);
	    */
		return 0;
	}
}

/**
  //上面是计算TF的已经玩车反转？？。结果为ID+value+TF
public static class HtmlMapper2 extends Mapper<LongWritable, Text,Text, Text>
{

public void map(LongWritable key, Text value, Context context
             ) throws IOException, InterruptedException {

	String val = value.toString().replace("	", " ");
	int index = val.indexOf(",");
	int index2 = val.indexOf(" ");
	String s1 = val.substring(0, index);//id
	String s2 = val.substring(index+1,index2);//word
	String s3 = val.substring(index2 + 1);//TF
	s3 = s3 + " ";
	s3=s3+"1";
	context.write(new Text(s1), new Text(s2+","+s3));
  }
 
 
}
public static class HtmlReducer2 
extends Reducer<Text, Text, Text, Text> {
	long docnum;
public void reduce(Text key, Text values, 
                Context context
                ) throws IOException, InterruptedException {
	float sum = 0;
	List<String> vals = new ArrayList<String>();
	for (Text val : values)
	{
		int index = val.toString().lastIndexOf(",");
		sum += Long.parseLong(val.toString().substring(index+1));
		vals.add(val.toString().substring(0,index));//保存单词
	}
	
	String val  = "";
	   context.write(key, new Text(val));
	   
	context.write(key, values);
}
}
 **/
