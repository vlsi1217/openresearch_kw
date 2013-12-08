package nju.lamda.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;

public class CopyFileInputFormat extends InputFormat<IntWritable, Text> 
{
	public CopyFileInputFormat()
	{
		
	}
    public List<InputSplit> getSplits(JobContext context)
            throws IOException, InterruptedException
	{
        
        String filename = context.getConfiguration().
        		get("input.copyfile.filename", "");
        Path path = new Path(StringUtils.unEscapeString(filename));
        int ncopy = context.getConfiguration().getInt(
        		"input.copyfile.number", 1);
        
        ArrayList<InputSplit>ret = new ArrayList<InputSplit>();
        FileSystem fs = path.getFileSystem(context.getConfiguration());
        FileStatus file = fs.getFileStatus(path);
        
        long flen = file.getLen();
        
        for(int i=0;i<ncopy;i++)
        {
        	ret.add(new CopyFileSplit(path,flen,i));
        }

		return ret;
	}

    public RecordReader<IntWritable, Text> createRecordReader(
    		InputSplit inputsplit, TaskAttemptContext taskattemptcontext) 
    		throws IOException, InterruptedException
	{
		return new CopyFileRecordReader();
	}
}
