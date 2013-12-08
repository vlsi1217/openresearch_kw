package baidu.openresearch.kw.feature;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import baidu.openresearch.kw.Constants;

public class AddIdToKeyword 
{
	public static void run(String dst) throws IOException
	{
		FileSystem fs = FileSystem.get(new Configuration());
		
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(
						fs.open(new Path(Constants.FILE_KEYWORD_CLASS))));
		BufferedWriter writer = new BufferedWriter(
				new OutputStreamWriter(
						fs.create(new Path(dst))));
		
		String line;
		int cnt=1;
		while( (line = reader.readLine())!=null)
		{
			line = line.trim();
			if ( line.length() > 0)
			{
				writer.write("" + cnt + "\t" + line);
				writer.write('\n');
				cnt++;
			}
		}
		
		writer.close();
		reader.close();
		fs.close();				
	}
	
	public static void main(String args[]) throws IOException
	{
		run(args[0]);
	}
}
