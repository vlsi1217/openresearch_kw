package baidu.openresearch.kw.feature;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import nju.lamda.hadoop.UtilsMapReduce;
import nju.lamda.svm.SvmIo;
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

public class KeywordSelection extends Configured implements Tool {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		private Map<Integer, Set<Integer>> map;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();

			map = new HashMap<Integer, Set<Integer>>();
			float thresh = conf.getFloat("keywordselection.thresh", 1.0f);

			try {
				Path pathModel = DistributedCache.getLocalCacheFiles(conf)[0];

				BufferedReader r = new BufferedReader(new FileReader(
						pathModel.toString()));
				String line;

				line = r.readLine();
				Scanner scanner = new Scanner(line);
				int numDim = scanner.nextInt();
				scanner.nextInt();
				double bias = scanner.nextDouble();

				int wDim = numDim;
				if (bias > 0) {
					wDim++;
				}
				scanner.close();

				while ((line = r.readLine()) != null) {
					Scanner sc = new Scanner(line);
					int label = sc.nextInt();

					for (int i = 0; i < wDim; i++) {
						double wi = sc.nextDouble();
						if (wi > thresh) {
							Set<Integer> set = map.get(label);
							if (set == null) {
								set = new HashSet<Integer>();
							}
							set.add(i + 1);
							map.put(label, set);
						}
					}
					sc.close();
				}
				r.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			int tpos = line.indexOf('\t');
			String id = line.substring(0, tpos);
			String str = line.substring(tpos + 1);

			SvmRecord rec = SvmIo.Parse(str);
			Set<Integer> set = map.get(rec.label);

			ArrayList<FeatureNode> list = new ArrayList<FeatureNode>();
			for (FeatureNode f : rec.feature) {
				if (set.contains(f.index)) {
					list.add(f);
				}
			}

			outputKey.set(id);
			if (list.size() > 0) {
				rec.feature = list.toArray(new FeatureNode[0]);
			} else {
				context.getCounter(KeywordSelection.class.getName(),
						"zerofeatures").increment(1);
			}

			outputValue.set(SvmIo.ToString(rec));

			context.write(outputKey, outputValue);
		}
	}

	public static class MyReducer extends
			Reducer<LongWritable, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			context.write(key, values.iterator().next());
		}
	}

	private String filenameFeature;
	private String filenameModel;
	private double thresh;
	private String filenameResult;
	private String workingDir;
	private int zeroFeatures;
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf,KeywordSelection.class.getName());
		job.setJarByClass(KeywordSelection.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(50);
        
        FileInputFormat.addInputPath(job, new Path(this.filenameFeature));
        
        Path output = new Path(workingDir + "/" + Constants.OUTDIR_TMP);
        
        conf = job.getConfiguration();
        
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(output);
        fs.close();
        FileOutputFormat.setOutputPath(job, output);
        
        conf.setFloat("keywordselection.thresh", (float)this.thresh);
        DistributedCache.addCacheFile(new Path(this.filenameModel).toUri(), conf);
        
        
        if (!job.waitForCompletion(true))
        {
        	return 1;
        }

        this.zeroFeatures = (int) job.getCounters().findCounter(
        		KeywordSelection.class.getName(),"zerofeatures").getValue();
        UtilsMapReduce.CopyResult(conf, output
        		, new Path(this.filenameResult), false);
        
		return 0;
	}
	
	public int run(String filefea, String filemodel, double thresh,
			String fileresult,String workdir) throws Exception
	{
		this.filenameFeature = filefea;
		this.filenameModel = filemodel;
		this.thresh = thresh;
		this.filenameResult = fileresult;
		this.workingDir = workdir;
		
		int retcd = ToolRunner.run(this, null);
		System.out.println(this.zeroFeatures);
		return retcd;
	}
	
	public static void main(String args[]) throws NumberFormatException, Exception
	{
		(new KeywordSelection()).run(args[0],args[1],Double.parseDouble(args[2])
				,args[3],args[4]);
	}
}
