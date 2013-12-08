package nju.lamda.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CopyFileRecordReader extends RecordReader<IntWritable, Text> 
{
	private long _pos;
	private BufferedReader _reader;
	private long _length;
	
	private IntWritable key = new IntWritable();
	private Text value = new Text();
	
    public void initialize(InputSplit inputsplit, 
    		TaskAttemptContext taskattemptcontext) 
    				throws IOException, InterruptedException
    {
    	CopyFileSplit split = (CopyFileSplit) inputsplit;
    	Path path = split.getPath();
    	
    	Configuration conf = taskattemptcontext.getConfiguration();
    	
    	FileSystem fs = path.getFileSystem(conf);
    	_reader = new BufferedReader(new InputStreamReader(fs.open(path)));
    	_pos = 0;
    	key.set(split.getIndex());
    	_length = split.getLength();
    }
    
    public boolean nextKeyValue()
            throws IOException, InterruptedException
    {
    	if ( _reader == null)
    	{
    		return false;
    	}
    	
    	String line = _reader.readLine();
    	
    	if ( line == null)
    	{
    		_reader.close();
    		_reader = null;
    		
    		return false;
    	}
    	
    	_pos += line.length();
    	
    	value.set(line);
    	
    	return true;
    }
    
    public IntWritable getCurrentKey()
            throws IOException, InterruptedException
    {
    	return key;
    }
    
    public Text getCurrentValue()
            throws IOException, InterruptedException
    {
    	return value;
    }
    
    public float getProgress()
            throws IOException, InterruptedException
    {
    	return Math.min(1.0f, ((float)_pos) / _length);
    }
    
    public void close() throws IOException
    {
    	if ( _reader != null)
    	{
    		_reader.close();
    	}
    }
            
}
