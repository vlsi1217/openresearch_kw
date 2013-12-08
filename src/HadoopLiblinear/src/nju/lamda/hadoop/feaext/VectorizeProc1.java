package nju.lamda.hadoop.feaext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class VectorizeProc1 extends Configured implements Tool {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		private char type;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			String filenameDict = conf.get("filename.dict");
			String filenameText = conf.get("filename.text");

			FileSplit split = (FileSplit) context.getInputSplit();
			String fname = split.getPath().getName();

			if (filenameDict.indexOf(fname) >= 0) {
				type = 'D';
			} else if (filenameText.indexOf(fname) >= 0) {
				type = 'T';
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			line = line.trim();
			if (line.length() > 0) {
				if (type == 'T') {
					int tpos = line.indexOf('\t');
					if ( tpos > 1)
					{
						String id = line.substring(0, tpos);
						String words[] = line.substring(tpos + 1).split(" ");
						HashMap<String, Integer> wmap = new HashMap<String, Integer>();
	
						double tcnt = 0.0;
						for (String w : words) {
							if (w.length() > 0) {
								int cnt = wmap.containsKey(w) ? wmap.get(w) : 0;
								wmap.put(w, cnt + 1);
								tcnt++;
							}
						}
	
						for (Entry<String, Integer> entry : wmap.entrySet()) {
							outputKey.set(entry.getKey() + "\t2");
							outputValue.set("" + id + ":"
									+ (entry.getValue() / tcnt));
							context.write(outputKey, outputValue);
						}
					}
					else
					{

					}
				} else if (type == 'D') {
					String parts[] = line.split("\t");
					outputKey.set(parts[1] + "\t1");
					outputValue.set(parts[0] + "\t" + parts[2]);
					context.write(outputKey, outputValue);
				}
			}
		}
	}

	public static class MyReducer extends
			Reducer<Text, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		private String dictId ="";
		private double idf;
		private String currentKey = "";
		private boolean binary;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			
			this.binary = conf.getBoolean("feature.binary", false); 
		}

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			String strLine = key.toString().trim();
			String strParts[] = strLine.split("\t");
			String strKey = strParts[0];
			String strKeypost = strParts[1].trim();
			
			if ( strKeypost.equals("1"))
			{
				String line = values.iterator().next().toString();
				String parts[] = line.split("\t");
				this.dictId = parts[0];
				this.idf = Double.parseDouble(parts[1]);
				this.currentKey = strKey;				
			}
			else if ( strKeypost.equals("2"))
			{
				if ( strKey.compareTo(this.currentKey) == 0)
				{
					outputKey.set(this.dictId);
					for (Text val : values) {
						String line = val.toString();
						String parts[] = line.split(":");
						String id = parts[0];
						Double tf = Double.parseDouble(parts[1]);
						if ( this.binary)
						{
							outputValue.set(id+":1");
						}
						else
						{
							outputValue.set(id+":"+(tf*this.idf));
						}
								
						context.write(outputKey, outputValue);
					}					
				}
			}
			else
			{
				throw new IOException("key post fix is wrong " + strKeypost);
			}
		}
	}
	
	public static class MyPartitioner extends HashPartitioner<Text,Text>
	{

		@Override
		public int getPartition(Text key, Text value, int numReducer) {
			String str = key.toString();
			
			return super.getPartition(new Text(str.split("\t")[0]), value, numReducer);
		}

		
	}

	public String filenameText;
	public String filenameDict;
	public boolean isBinary;
	public String workingDir;
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, VectorizeProc1.class.getName());
		job.setJarByClass(VectorizeProc1.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(50);

        FileInputFormat.addInputPath(job, new Path(this.filenameText));
        FileInputFormat.addInputPath(job, new Path(this.filenameDict));
        
        Path output = new Path(workingDir + "/" + Constants.OUTDIR_TMP);
        
        conf = job.getConfiguration();
        
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(output);
        fs.close();
        FileOutputFormat.setOutputPath(job, output);

        conf.set("filename.dict", this.filenameDict);
        conf.set("filename.text", this.filenameText);
        conf.setBoolean("feature.binary", this.isBinary);
        conf.setInt("mapred.map.max.attempts", 10);
        conf.setInt("mapred.reduce.max.attempts", 10);  
        
		if (!job.waitForCompletion(true)) {
			return 1;
		}

		return 0;
	}
	
	public int run(String filetext, String filedict, 
			boolean binary, String workdir) throws Exception
	{
		this.filenameText = filetext;
		this.filenameDict = filedict;
		this.isBinary = binary;
		this.workingDir = workdir;
		
		return ToolRunner.run(this, null);
	}

}
