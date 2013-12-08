package nju.lamda.hadoop.feaext;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.TreeMap;

import nju.lamda.hadoop.UtilsFileSystem;
import nju.lamda.hadoop.UtilsMapReduce;
import nju.lamda.svm.SvmIo;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.bwaldvogel.liblinear.FeatureNode;

public class MergeLabelFeatures extends Configured implements Tool {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		private ArrayList<String> listFiles;
		private ArrayList<Integer> listIdx;
		private String labelFile;

		private int type;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			listFiles = new ArrayList<String>();
			listIdx = new ArrayList<Integer>();

			try {
				Path pathinfo = DistributedCache.getLocalCacheFiles(conf)[0];

				BufferedReader r = new BufferedReader(new FileReader(
						pathinfo.toString()));

				labelFile = r.readLine().trim();

				String line;
				while ((line = r.readLine()) != null) {
					if ( line.length() > 0)
					{
						String parts[] = line.split("\t");
						listFiles.add(parts[0]);
						listIdx.add(Integer.parseInt(parts[1]));
					}
				}
				r.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			FileSplit split = (FileSplit) context.getInputSplit();
			String fname = split.getPath().getName();

			if (labelFile.indexOf(fname) >= 0) {
				type = 0;
			} else {
				int cnt = 1;
				for (String file : listFiles) {
					if (file.indexOf(fname) >= 0) {
						type = cnt;
						break;
					}
					cnt++;
				}
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if (type == 0) {// it is label
				String parts[] = line.split("[\\t ]");
				outputKey.set(parts[0]);
				outputValue.set("L\t" + parts[1]);
				context.write(outputKey, outputValue);
			} else {
				String pairs[] = line.split("[\\t ]");

				String id = pairs[0];
				int flen = pairs.length - 1;

				FeatureNode feature[] = new FeatureNode[flen];

				int i;
				for (i = 1; i < pairs.length; i++) {
					String pair[] = pairs[i].split(":");
					int index = Integer.parseInt(pair[0])
							+ listIdx.get(type - 1);
					double val = Double.parseDouble(pair[1]) / listIdx.size();
					feature[i - 1] = new FeatureNode(index, val);
				}

				outputKey.set(id);
				outputValue.set("F" + type + "\t" + SvmIo.ToString(feature));

				context.write(outputKey, outputValue);
			}
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		private Text outputValue = new Text();

		public void setup(Context context) {

		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String label="";
			TreeMap<Integer,String> map = new TreeMap<Integer,String>();
			for(Text val : values)
			{
				String str = val.toString();
				char ch = str.charAt(0);
				
				if ( ch == 'L')
				{
					label = str.split("\t")[1];
				}
				else if ( ch == 'F')
				{
					int tpos = str.indexOf('\t');
					int idx = Integer.parseInt(str.substring(1, tpos));
					
					map.put(idx, str.substring(tpos+1));
				}
			}
			
			StringBuffer buf = new StringBuffer();
			buf.append(label);

			for(Entry<Integer,String> entry : map.entrySet())
			{
				buf.append(" ");
				buf.append(entry.getValue());
			}
			
			outputValue.set(buf.toString().trim());
			context.write(key, outputValue);
		}
	}

	private ArrayList<String> listFiles;
	private String labelFile;
	private String filenameResult;
	private String workingDir;
	private static final String FILENAME_TMP = "tmp_MergeLabelFeatures";
	private String filenameTemp;
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, MergeLabelFeatures.class.getName());
		job.setJarByClass(MergeLabelFeatures.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(50);
		
		FileInputFormat.addInputPath(job, new Path(labelFile));
		for(String file : listFiles)
		{
			FileInputFormat.addInputPath(job, new Path(file));
		}
		
        Path output = new Path(workingDir + "/" + Constants.OUTDIR_TMP);
        
        conf = job.getConfiguration();
        
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(output);
        fs.close();
        FileOutputFormat.setOutputPath(job, output);
        
        DistributedCache.addCacheFile(new Path(this.filenameTemp).toUri(), conf);
        
		if (!job.waitForCompletion(true)) {
			return 1;
		}
		
		UtilsMapReduce.CopyResult(conf, output, new Path(
				this.filenameResult)
			, false);	
		return 0;
	}
	
	public int run(String labelfile, ArrayList<String> files, 
			ArrayList<Integer> idx, String fileresult, String workdir) 
					throws Exception
	{
		this.labelFile = labelfile;
		this.listFiles = files;
		this.filenameResult = fileresult;
		this.workingDir = workdir;
		
		this.filenameTemp = workdir + "/" + FILENAME_TMP;
		
		BufferedWriter w = UtilsFileSystem.GetWriter(new Path(filenameTemp));
		
		w.write(labelfile);
		w.write("\n");
		for(int i=0;i<listFiles.size();i++)
		{
			w.write(listFiles.get(i));
			w.write("\t");
			w.write(""+idx.get(i));
			w.write("\n");
		}
		
		w.close();
		return ToolRunner.run(this,null);
	}
	
	public static void main(String args[]) throws Exception
	{
		String labelfile = args[0];
		ArrayList<Integer> idx = new ArrayList<Integer>();
		ArrayList<String> files = new ArrayList<String>();
		
		String fileresult = args[1];
		String workdir = args[2];
		
		int i=3;
		while( i< args.length)
		{
			files.add(args[i++]);
			idx.add(Integer.parseInt(args[i++]));
		}
		
		(new MergeLabelFeatures()).run(labelfile, files, idx, fileresult, workdir);
	}
}
