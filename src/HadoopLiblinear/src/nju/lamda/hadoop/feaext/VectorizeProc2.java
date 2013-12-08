package nju.lamda.hadoop.feaext;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import nju.lamda.hadoop.UtilsMapReduce;
import nju.lamda.svm.SvmIo;
import nju.lamda.svm.SvmOp;

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

import de.bwaldvogel.liblinear.FeatureNode;

public class VectorizeProc2 extends Configured implements Tool {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		public void setup(Context context) {
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			line = line.trim();
			if (line.length() > 0) {
				String parts[] = line.split("\t");

				if ( parts.length != 2)
				{
					throw new IOException("parts' length musth be 2");
				}
				
				String pair[] = parts[1].split(":");
				outputKey.set(pair[0]);
				outputValue.set(parts[0]+":"+pair[1]);
				context.write(outputKey, outputValue);
			}
		}
	}

	public static class MyReducer extends
			Reducer<Text, Text, Text, Text> {
		private Text outputValue = new Text();

		public void setup(Context context) {

		}

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			TreeMap<Integer,Double> map = new TreeMap<Integer,Double>();
			for( Text val : values)
			{
				String parts[] = val.toString().split(":");
				map.put(Integer.parseInt(parts[0]), Double.parseDouble(parts[1]));
			}
			
			FeatureNode x[] = new FeatureNode[map.size()];
			
			int i=0;
			for( Entry<Integer,Double> entry : map.entrySet())
			{
				x[i++] = new FeatureNode(entry.getKey(),entry.getValue());
			}
			
			SvmOp.normalizeL2(x);
			
			outputValue.set(SvmIo.ToString(x));
			context.write(key, outputValue);
		}
	}

	public String filenameResult;
	public String workingDir;
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, VectorizeProc2.class.getName());
		job.setJarByClass(VectorizeProc2.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(50);
		
		FileInputFormat.addInputPath(job, new Path(workingDir + "/" +
				Constants.OUTDIR_TMP));
        
        Path output = new Path(workingDir + "/" + Constants.OUTDIR_TMP2);
        
        conf = job.getConfiguration();
        
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(output);
        fs.close();
        FileOutputFormat.setOutputPath(job, output);

        
		if (!job.waitForCompletion(true)) {
			return 1;
		}
		
		UtilsMapReduce.CopyResult(conf, output, new Path(
				this.filenameResult)
			, false);
		
		return 0;
	}
	
	public int run(String fileresult, String workdir) throws Exception
	{
		this.filenameResult = fileresult;
		this.workingDir = workdir;
		
		return ToolRunner.run(this, null);
	}

}
