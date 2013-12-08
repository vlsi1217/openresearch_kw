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

public class SegmentTitle extends Configured implements Tool {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		private WordSegmenterBase segmenter;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			int type = conf.getInt("segmenter.type", 0);
			
			if (type == 0)
			{
				segmenter = new WordSegmenterIKAnalyzer();
			}
			else if ( type == 1)
			{
				segmenter = new WordSegmenterChar();
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			if (line.length() > 0) {
				String pair[] = line.split("\t");

				if ((pair.length == 11) ||(pair.length == 10)) {
					outputKey.set(pair[0]);
					StringBuffer buffer = new StringBuffer();
					for (int i = 1; i < pair.length; i++) {
						if (pair[i].trim().length() > 0)
						{
							String words[] = segmenter.segment(pair[i]);
							for( String w : words)
							{
								if ( w.trim().length() > 0)
								{
									buffer.append(w);
									buffer.append(' ');
								}
							}
						}
					}

					outputValue.set(buffer.toString().trim());
					context.write(outputKey, outputValue);
				}
				else {// only label
					throw new IOException("parts must be 4");
				}
			}
		}
	}

	public static class MyReducer extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			context.write(key, values.iterator().next());
		}
	}

	public String filenameResult;
	public int type;
	public String workingDir;
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf,SegmentTitle.class.getName());
		job.setJarByClass(SegmentTitle.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(50);
        
        FileInputFormat.addInputPath(job, new Path(Constants.FILE_KEYWORD_TITLE));
        //FileInputFormat.setMaxInputSplitSize(job, 1024*1024*16);
        
        Path output = new Path(workingDir + "/" + Constants.OUTDIR_TMP);
        
        conf = job.getConfiguration();
        
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(output);
        fs.close();
        FileOutputFormat.setOutputPath(job, output);
        
        conf.setInt("segmenter.type", this.type);
        UtilsMapReduce.AddLibraryPath(conf, new Path("lib"));
        
        if (!job.waitForCompletion(true))
        {
        	return 1;
        }
		
        UtilsMapReduce.CopyResult(conf, output
        		, new Path(this.filenameResult), false);
        
		return 0;
	}
	
	public int run(String fileResult, int type, String workdir) throws Exception
	{
		this.filenameResult = fileResult;
		this.type = type;
		this.workingDir = workdir;
		
		return ToolRunner.run(this, null);
	}
	
	public static void main(String args[]) throws Exception
	{
		(new SegmentTitle()).run(args[0],Integer.parseInt(args[1]), args[2]);
	}
}
