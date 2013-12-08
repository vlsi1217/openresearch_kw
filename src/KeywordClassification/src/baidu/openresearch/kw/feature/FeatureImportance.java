package baidu.openresearch.kw.feature;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import nju.lamda.hadoop.UtilsMapReduce;
import nju.lamda.hadoop.liblinear.SvmRecordHadoop;
import nju.lamda.svm.SvmIo;
import nju.lamda.svm.SvmOp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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

import de.bwaldvogel.liblinear.FeatureNode;

public class FeatureImportance extends Configured implements Tool {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		private HashMap<Integer, Double> map;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();

			map = new HashMap<Integer, Double>();
			try {
				Path pathDict = DistributedCache.getLocalCacheFiles(conf)[0];
				String line;
				BufferedReader r = new BufferedReader(new FileReader(
						pathDict.toString()));

				while ((line = r.readLine()) != null) {
					String parts[] = line.split("\t");
					int index = Integer.parseInt(parts[0]);
					double imp = Math.log(Double.parseDouble(parts[1]));

					map.put(index, imp);
				}
				r.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			SvmRecordHadoop rec = SvmRecordHadoop.FromText(value);

			FeatureNode x[] = new FeatureNode[rec.rec.feature.length];
			int cnt = 0;
			for (FeatureNode f : rec.rec.feature) {
				Double imp = map.get(f.index);
				if (imp == null) {
					imp = 1.0;
				}
				x[cnt] = new FeatureNode(f.index, imp);
				cnt++;
			}

			SvmOp.normalizeL2(x);
			outputKey.set(rec.id);
			outputValue.set(SvmIo.ToString(x));
			context.write(outputKey, outputValue);
		}
	}

	public static class MyReducer extends
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			context.write(key, values.iterator().next());
		}
	}

	public String filenameInput;
	public String filenameDict;
	public String filenameResult;
	public String workingDir;
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf,FeatureImportance.class.getName());
		job.setJarByClass(FeatureImportance.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setNumReduceTasks(50);
        FileInputFormat.addInputPath(job, new Path(this.filenameInput));
        
        Path output = new Path(workingDir + "/" + Constants.OUTDIR_TMP);
        
        conf = job.getConfiguration();
        
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(output);
        fs.close();
        FileOutputFormat.setOutputPath(job, output);
        
        DistributedCache.addCacheFile((new Path(this.filenameDict)).toUri()
        		, conf);
        
        if (!job.waitForCompletion(true))
        {
        	return 1;
        }
		
        UtilsMapReduce.CopyResult(conf, output
        		, new Path(this.filenameResult), false);
        
		return 0;
	}

	public int run(String filein, String filedict, String fileres, String workdir) throws Exception
	{
		this.filenameInput = filein;
		this.filenameDict = filedict;
		this.filenameResult = fileres;
		this.workingDir = workdir;
		
		return ToolRunner.run(this, null);
	}
	
	public static void main(String args[]) throws Exception
	{
		(new FeatureImportance()).run(args[0],args[1],args[2],args[3]);
	}
}
