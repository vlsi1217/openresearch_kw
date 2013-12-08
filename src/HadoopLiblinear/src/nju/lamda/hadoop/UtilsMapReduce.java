package nju.lamda.hadoop;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class UtilsMapReduce 
{
	public static void CopyResult( Configuration conf, Path result, Path dst,
			boolean append) 
			throws IOException
	{
		FileSystem fs = FileSystem.get(conf);
		
		Path pattern = new Path(result.toString() + "/part*");
		
		UtilsFileSystem.MergeFiles(fs, pattern, dst, append);
		
		fs.close();
	}
	
	public static void CopyResult( Configuration conf, Path result, 
			OutputStream dst) throws IOException
	{
		FileSystem fs = FileSystem.get(conf);
		Path pattern = new Path(result.toString() + "/part*");
		
		UtilsFileSystem.MergeFiles(fs, pattern, dst);
		
		fs.close();		
	}
	
	public static void AddLibraryPath(Configuration conf, Path libpath) throws IOException
	{
		FileSystem fs = FileSystem.get(conf);
		FileStatus files[] = fs.listStatus(libpath);
		
		for( FileStatus file : files)
		{
			DistributedCache.addArchiveToClassPath(file.getPath(), 
					conf, fs );
		}
		fs.close();
	}
}
