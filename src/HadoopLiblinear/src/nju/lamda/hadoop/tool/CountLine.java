package nju.lamda.hadoop.tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CountLine 
{
	public static void main(String args[]) throws IOException
	{
		Path input = new Path(args[0]);
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		BufferedReader reader = new BufferedReader(
				new InputStreamReader( fs.open(input)));
		int cnt = 0;
		
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
				cnt++;
			}
		}
		
		System.out.println(args[0]+" : "+cnt);
	}
}
