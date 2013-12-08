package nju.lamda.hadoop.tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

import nju.lamda.hadoop.UtilsFileSystem;

import org.apache.hadoop.fs.Path;

public class AddIdToFile 
{
	public static void run(String src, String dst) throws IOException
	{
		BufferedReader r = UtilsFileSystem.GetReader(new Path(src));
		BufferedWriter w = UtilsFileSystem.GetWriter(new Path(dst));
		
		String line;
		int cnt = 1;
		while( (line = r.readLine())!=null)
		{
			w.write("" + cnt + "\t" + line + "\n");
			cnt++;
		}
		
		r.close();
		w.close();
	}
	
	public static void main(String args[]) throws IOException
	{
		run(args[0],args[1]);
	}
}
