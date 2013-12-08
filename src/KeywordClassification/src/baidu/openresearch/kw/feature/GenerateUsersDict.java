package baidu.openresearch.kw.feature;

import java.io.IOException;

import nju.lamda.hadoop.UtilsMapReduce;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import baidu.openresearch.kw.Constants;

public class GenerateUsersDict extends Configured implements Tool
{
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();

		public void setup(Context context)
		{		

		}
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String pair[] = line.split("\t");
			
			if ( pair.length != 2)
			{
				return;
			}
			
			k.set(pair[1]);
			v.set(""+1);
			context.write(k, v);
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> 
	{
		private Text v = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int cnt = 0;
			for(Text val : values)
			{
				cnt += Integer.parseInt(val.toString());
			}
			
			if ( cnt > 1)
			{
				v.set("" + cnt);
			
				context.write(key, v);
			}
		}
	}
	
	public String filenameDict;
	public String workingDir;
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf,"GenerateUsersDict");
		job.setJarByClass(GenerateUsersDict.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
		
        FileInputFormat.addInputPath(job, new Path(Constants.FILE_KEYWORD_USERS));
        
        Path output = new Path(workingDir + "/" + Constants.OUTDIR_TMP);
        
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(output);
        fs.close();
        FileOutputFormat.setOutputPath(job, output);
        
        if (!job.waitForCompletion(true))
        {
        	return 1;
        }
		
        UtilsMapReduce.CopyResult(conf, output
        , new Path(this.filenameDict), false);
        
		return 0;
	}	
	
	public int run(String filedict, String workingdir) throws Exception
	{
		this.filenameDict = filedict;
		this.workingDir = workingdir;
		
		return ToolRunner.run(this, null);
	}
	
	public static void main(String args[]) throws Exception
	{
		GenerateUsersDict proc = new GenerateUsersDict();
		proc.run(args[0],args[1]);
	}

}
