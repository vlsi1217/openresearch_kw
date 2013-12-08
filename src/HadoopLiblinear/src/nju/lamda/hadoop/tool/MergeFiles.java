package nju.lamda.hadoop.tool;

import java.io.IOException;

import nju.lamda.hadoop.UtilsFileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MergeFiles 
{
	public static void main(String args[]) throws IOException
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		UtilsFileSystem.MergeFiles(fs, new Path(args[0]), new Path(args[1])
		, false);
		
		fs.close();
	}
}
