package baidu.openresearch.kw.feature;

import java.io.IOException;
import java.util.ArrayList;

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

public class ReplaceKeywordWithId extends Configured implements Tool {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		private char type = ' ';

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();

			FileSplit split = (FileSplit) context.getInputSplit();
			String filenameKeyword = conf.get("filename.keyword");
			String filenameData = conf.get("filename.data");

			String fname = split.getPath().getName();

			if (filenameKeyword.indexOf(fname) >= 0) {
				type = 'K';
			} else if (filenameData.indexOf(fname) >= 0) {
				type = 'D';
			}

		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			if (type == 'K') {
				String parts[] = line.split("\t");
				if(parts.length == 2){
					outputKey.set(parts[1]);
				}
				else
				{
					outputKey.set("");
				}
				outputValue.set("" + type + parts[0]);
				context.write(outputKey, outputValue);
			} else if (type == 'D') {
				int tpos = line.indexOf('\t');
				String kw = line.substring(0, tpos);
				String data = line.substring(tpos + 1);

				outputKey.set(kw);
				outputValue.set("" + type + data);
				context.write(outputKey, outputValue);
			}
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			ArrayList<String> listId = new ArrayList<String>();
			StringBuffer buf = new StringBuffer();
			
			for(Text val : values)
			{
				String line = val.toString();
				
				char ch = line.charAt(0);
				String str = line.substring(1);
				
				if ( ch == 'D')
				{
					buf.append( "\t" + str);
				}
				else if ( ch == 'K')
				{
					listId.add(str);
				}
			}
			
			String ov = buf.toString().trim();
			
			for( String id : listId)
			{
				outputKey.set(id);
				outputValue.set(ov);
				
				context.write(outputKey, outputValue);
			}
		}
	}

	private String filenameResult;
	private String filenameKeyword;
	private String filenameData;
	private String workingDir;
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf,ReplaceKeywordWithId.class.getName());
		job.setJarByClass(ReplaceKeywordWithId.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(50);
        
        FileInputFormat.addInputPath(job, new Path(this.filenameKeyword));
        FileInputFormat.addInputPath(job, new Path(this.filenameData));
        
        Path output = new Path(workingDir + "/" + Constants.OUTDIR_TMP);
        
        conf = job.getConfiguration();
        
        conf.set("filename.keyword", this.filenameKeyword);
        conf.set("filename.data", this.filenameData);
        
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(output);
        fs.close();
        FileOutputFormat.setOutputPath(job, output);
        
        if (!job.waitForCompletion(true))
        {
        	return 1;
        }
		
        UtilsMapReduce.CopyResult(conf, output
        		, new Path(this.filenameResult), false);
        
		return 0;
	}

	public int run(String filekw, String filedata, String fileresult
			, String workdir) throws Exception
	{
		this.filenameKeyword = filekw;
		this.filenameData = filedata;
		this.filenameResult = fileresult;
		this.workingDir = workdir;
		
		return ToolRunner.run(this, null);
	}
	
	public static void main(String args[]) throws Exception
	{
		(new ReplaceKeywordWithId()).run(args[0], args[1], args[2], args[3]);
	}
}
