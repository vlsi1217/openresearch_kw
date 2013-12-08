package baidu.openresearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Exec 
{
	public static void main(String args[]) throws IOException, InterruptedException
	{
		StringBuffer buf = new StringBuffer();
		for(int i=0;i<args.length;i++)
		{
			buf.append(args[i]);
			buf.append(" ");
		}
		Process p = Runtime.getRuntime().exec(buf.toString());
		p.waitFor();
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(p.getInputStream()));
		String line;
		
		while((line = reader.readLine())!= null)
		{
			System.out.println(line);
		}
		reader.close();
	}
}
