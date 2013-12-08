package baidu.openresearch.kw.feature;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;

import nju.lamda.common.Pair;
import nju.lamda.hadoop.UtilsMapReduce;
import nju.lamda.svm.SvmIo;
import nju.lamda.svm.SvmOp;
import nju.lamda.svm.SvmRecord;

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

public class FeatureKeyword extends Configured implements Tool {
	public static class MyMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {
		private LongWritable outputKey = new LongWritable();
		private Text outputValue = new Text();
		private HashMap<String, Pair<Integer, Double>> map;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();

			try {
				Path pathDict = DistributedCache.getLocalCacheFiles(conf)[0];
				BufferedReader reader = new BufferedReader(new FileReader(
						pathDict.toString()));
				String line;
				map = new HashMap<String, Pair<Integer, Double>>();

				int cnt = 1;
				while ((line = reader.readLine()) != null) {
					line = line.trim();
					if (line.length() > 0) {
						String pair[] = line.split("\t");
						map.put(pair[0], new Pair<Integer, Double>(cnt++,
								Double.parseDouble(pair[1])));
					}
				}

				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();

			FeatureNode x[] = null;
			String strLabel = "";
			if (line.length() > 0) {
				String parts[] = line.split("\t");
				if (parts.length == 4) {
					String words[] = parts[2].split("\\|");
					TreeSet<String> wordset = new TreeSet<String>();
					for( String w : words)
					{
						wordset.add(w);
					}
					
					ArrayList<FeatureNode> fs = new ArrayList<FeatureNode>();
					for (String w : wordset) {
						Pair<Integer, Double> val = map.get(w);
						if ( val != null)
						{
							fs.add(new FeatureNode(val.v1, val.v2));
						}
					}
					
					x = fs.toArray(new FeatureNode[0]);
					strLabel = parts[3];
				} else if (parts.length == 2) {
					x = new FeatureNode[0];
					strLabel = parts[1];
				}

				SvmOp.normalizeL2(x);
				int label = -999;
				if (Character.isDigit(strLabel.charAt(0))) {
					label = Integer.parseInt(strLabel);
				}
				SvmRecord rec = new SvmRecord(label, x);
				outputKey.set(Long.parseLong(parts[0]));
				outputValue.set(SvmIo.ToString(rec));
				context.write(outputKey, outputValue);
			}
		}
	}

	public static class MyReducer extends
			Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			context.write(key, values.iterator().next());
		}
	}

	public String filenameKeyword;
	public String filenameDict;
	public String filenameResult;
	public String workingDir;
	
	@Override
	public int run(String[] as) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf,FeatureKeyword.class.getName());
		job.setJarByClass(FeatureKeyword.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(this.filenameKeyword));
        FileInputFormat.setMaxInputSplitSize(job, 1024*1024*3);
        
        Path output = new Path(workingDir + "/" + Constants.OUTDIR_TMP);
        
        conf = job.getConfiguration();
        
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(output);
        fs.close();
        FileOutputFormat.setOutputPath(job, output);
        
        DistributedCache.addCacheFile((new Path(this.filenameDict)).toUri()
        		, conf);
        UtilsMapReduce.AddLibraryPath(conf, new Path("lib"));
        
        if (!job.waitForCompletion(true))
        {
        	return 1;
        }
		
        UtilsMapReduce.CopyResult(conf, output
        		, new Path(this.filenameResult), false);
        
		return 0;

	}
	
	public int run(String filekeyword, String filedict, 
			String fileresult, String workingdir) throws Exception
	{
		this.filenameKeyword = filekeyword;
		this.filenameDict = filedict;
		this.filenameResult = fileresult;
		this.workingDir = workingdir;
		
		return ToolRunner.run(this, null);
	}
	
	public static void main(String args[]) throws Exception
	{
		(new FeatureKeyword()).run(args[0],args[1],args[2],args[3]);
	}
}
