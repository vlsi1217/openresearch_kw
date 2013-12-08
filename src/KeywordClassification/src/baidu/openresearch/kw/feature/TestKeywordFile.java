package baidu.openresearch.kw.feature;

import java.io.BufferedReader;
import java.io.IOException;

import org.apache.hadoop.fs.Path;

import nju.lamda.hadoop.UtilsFileSystem;

public class TestKeywordFile 
{
	public static void main(String args[]) throws IOException
	{
		BufferedReader reader = UtilsFileSystem.GetReader(new Path(args[0]));
		
		String line;
		int cnt = 0;
		int num = 1;
		while( (line = reader.readLine())!=null)
		{
			line = line.trim();
			String parts[] = line.split("\t");
			if( parts.length != 3)
			{
				cnt ++;
				System.out.println("" + num + "," + line);
			}
			
			num++;
		}
		
		System.out.println("total: " + cnt);
	}
}
