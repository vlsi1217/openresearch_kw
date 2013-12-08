package baidu.openresearch.kw.feature;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

import nju.lamda.hadoop.UtilsFileSystem;

import org.apache.hadoop.fs.Path;

public class TestTitleFile 
{
	public static void main(String args[]) throws IOException
	{
		BufferedReader reader = UtilsFileSystem.GetReader(new Path(args[0]));
		BufferedWriter writer = UtilsFileSystem.GetWriter(new Path(args[1]));
		String line;
		int cnt = 0;
		int num = 1;
		while( (line = reader.readLine())!=null)
		{
			line = line.trim();
			String parts[] = line.split("\t");
			if( parts.length != 11)
			{
				cnt ++;
				System.out.println("parts: " + parts.length);
				writer.write("" + num + "," +line);
				writer.write('\n');
			}
			
			num++;
		}
		
		System.out.println("total: " + cnt);
		writer.close();
		reader.close();
	}
}
