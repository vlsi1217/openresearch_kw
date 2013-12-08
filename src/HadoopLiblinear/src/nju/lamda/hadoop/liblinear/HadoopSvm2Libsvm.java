package nju.lamda.hadoop.liblinear;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopSvm2Libsvm 
{
	public static void run(String filehadoop, String filesvm) throws IOException
	{
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);	
		
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(fs.open(new Path(filehadoop))));
		BufferedWriter writer = new BufferedWriter(
				new OutputStreamWriter(fs.create(new Path(filesvm))));
		
		while(true)
		{
			String line = reader.readLine();
			if ( line == null)
			{
				break;
			}
			line = line.trim();
			
			if ( line.length() > 0)
			{
				int tpos = line.indexOf('\t');
				String substr = line.substring(tpos+1);
				writer.write(substr);
				writer.write('\n');
			}
		}
		
		reader.close();
		writer.close();
		fs.close();
	}
	
	public static void main(String args[]) throws IOException
	{
		run(args[0],args[1]);
	}
}
