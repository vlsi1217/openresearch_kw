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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import baidu.openresearch.kw.Constants;

public class RemoveDuplicateDict extends Configured implements Tool {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		int type = 0;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			
			String fnameDict1 = conf.get("filename.dict1");
			String fnameDict2 = conf.get("filename.dict2");
			
			FileSplit split = (FileSplit) context.getInputSplit();
			String fname = split.getPath().getName();
			
			if ( fnameDict1.indexOf(fname) >= 0)
			{
				type = 1;
			}
			else if ( fnameDict2.indexOf(fname) >=0)
			{
				type = 2;
			}
			
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String parts[] = line.split("\t");

			outputKey.set(parts[1]);
			outputValue.set("" + type + parts[0] + "\t" + parts[2]);
			context.write(outputKey, outputValue);
		}
	}

	public static class MyReducer extends
			Reducer<Text, Text, Text, Text> {
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int cnt = 0;
			String line = "";
			for( Text val : values)
			{
				line = val.toString();
				cnt++;
			}
			
			if ( cnt == 1)
			{
				if ( line.charAt(0) == '2')
				{
					line = line.substring(1);
					String parts[] = line.split("\t");
					outputKey.set( parts[0]);
					outputValue.set(key.toString() + "\t" + parts[1]);
					
					context.write(outputKey, outputValue);
				}
			}
		}
	}

	public String filenameDict1;
	public String filenameDict2;
	
	public String workingDir;
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf,RemoveDuplicateDict.class.getName());
		job.setJarByClass(RemoveDuplicateDict.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(this.filenameDict1));
        FileInputFormat.addInputPath(job, new Path(this.filenameDict2));
        
        Path output = new Path(workingDir + "/" + Constants.OUTDIR_TMP);
        
        conf = job.getConfiguration();
        
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(output);
        fs.close();
        FileOutputFormat.setOutputPath(job, output);   
        
        conf.set("filename.dict1", this.filenameDict1);
        conf.set("filename.dict2", this.filenameDict2);
        
        if (!job.waitForCompletion(true))
        {
        	return 1;
        }

        UtilsMapReduce.CopyResult(conf, output
        		, new Path(this.filenameDict2), false);
        
		return 0;
	}
	
	public int run(String filedict1, String filedict2, String workdir) 
			throws Exception
	{
		this.filenameDict1 = filedict1;
		this.filenameDict2 = filedict2;
		
		this.workingDir = workdir;
		
		return ToolRunner.run(this, null);
	}
	
	public static void main(String args[]) throws Exception
	{
		(new RemoveDuplicateDict()).run(args[0],args[1],args[2]);
	}

}
