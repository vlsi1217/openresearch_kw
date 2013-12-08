package baidu.openresearch.kw;

import java.io.IOException;
import java.util.TreeSet;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestKeywordInUser extends Configured implements Tool
{
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();

		public void setup(Context context)
		{		
			FileSplit split = (FileSplit) context.getInputSplit();
			
			if ( split.getPath().getName().indexOf("user") > 0)
			{
				v.set("user");
			}
			else if ( split.getPath().getName().indexOf("class") > 0)
			{
				v.set("keyword");
			}
			else
			{
				//
			}
		}
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String pair[] = line.split("\t");
			
			k.set(pair[0]);
			context.write(k, v);
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> 
	{
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			TreeSet<String> set = new TreeSet<String>();
			for(Text val : values)
			{
				set.add(val.toString());
			}
			
			if ( set.size() ==2)
			{
				context.getCounter("TestKeywordInUser", "exist").increment(1);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf,"TestKeywordInUser");
		job.setJarByClass(TestKeywordInUser.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
		
        FileInputFormat.addInputPath(job, new Path(Constants.FILE_KEYWORD_CLASS));
        FileInputFormat.addInputPath(job, new Path(Constants.FILE_KEYWORD_USERS));
        Path output = new Path("temp");
        
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(output);
        fs.close();
        FileOutputFormat.setOutputPath(job, output);
        
        if (!job.waitForCompletion(true))
        {
        	return 1;
        }
		
        System.out.println(job.getCounters().
        		findCounter("TestKeywordInUser", "exist").getValue());
		return 0;
	}
	
	public static void main(String args[]) throws Exception
	{
		ToolRunner.run(new TestKeywordInUser(), args);
	}
	
}
