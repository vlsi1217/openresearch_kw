package nju.lamda.hadoop.liblinear;

import java.io.IOException;

import nju.lamda.hadoop.UtilsMapReduce;
import nju.lamda.svm.SvmRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TrainPreproc extends Configured implements Tool {

	public int maxIndex;
	public int numClass;
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			SvmRecordHadoop recHadoop = SvmRecordHadoop.FromText(value);
			SvmRecord rec = recHadoop.rec;
			int flen = rec.feature.length;
			k.set("L" + rec.label);
			v.set("");
			context.write(k, v);
			context.getCounter("TrainPreproc", "ndata").increment(1);
			
			if ( flen > 0)
			{
				int maxIndex = rec.feature[flen - 1].index;
		
				k.set("I");
				v.set(""+maxIndex);
				context.write(k, v);
			}
		}
	}

	public static class MyCombiner extends Reducer<Text, Text, Text, Text> {
		private Text v = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			if (key.toString().charAt(0) == 'I') 
			{
				int maxIndex = 0;
				for (Text text : values) 
				{
					maxIndex = Math.max(maxIndex,
							Integer.parseInt(text.toString()));
				}

				v.set("" + maxIndex);
				context.write(key, v);		
			} 
			else
			{
				for(Text val : values)
				{
					context.write(key, val);
				}
			}
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> 
	{
		private Text k = new Text();
		private Text v = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String strKey = key.toString();
			
			if (strKey.charAt(0) == 'I') {
				int maxIndex = 0;
				for (Text text : values) {
					maxIndex = Math.max(maxIndex,
							Integer.parseInt(text.toString()));
				}

				context.getCounter("TrainPreproc", "maxindex").setValue(maxIndex);
				
			} else if (strKey.charAt(0) == 'L') {
				int label = Integer.parseInt(strKey.substring(1));
				
				k.set("" + label);
				context.write(k, v);
				context.getCounter("TrainPreproc", "nlabel").increment(1);
			}
		}
	}

	public int run(String args[]) throws Exception
    {
		Configuration conf = getConf();
		Job job = new Job(conf,"SVM Train Preprocess");
		job.setJarByClass(TrainPreproc.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        conf = job.getConfiguration();
        Path output = new Path( args[2] + "/" + Constants.OUTDIR_SVM_PRETRAIN);
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(output);
        fs.close();
        
        FileOutputFormat.setOutputPath(job, output);
        //UtilsMapReduce.AddLibraryPath(conf, new Path("lib"));
        if (!job.waitForCompletion(true))
        {
        	return 1;
        }
        
        UtilsMapReduce.CopyResult(conf, output, new Path(args[1]), false);
        this.maxIndex = (int) job.getCounters().
        		findCounter("TrainPreproc", "maxindex").getValue();
        this.numClass = (int) job.getCounters().
        		findCounter("TrainPreproc", "nlabel").getValue();
        
        return 0;
    }
	
	public int run(String datafile, String labelfile, String workingdir) 
			throws Exception
	{
		String args[] = new String[3];
		args[0] = datafile;
		args[1] = labelfile;
		args[2] = workingdir;
		
		return ToolRunner.run(this, args);
	}
}
