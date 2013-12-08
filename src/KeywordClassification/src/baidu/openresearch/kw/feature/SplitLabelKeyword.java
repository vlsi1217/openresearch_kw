package baidu.openresearch.kw.feature;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

import org.apache.hadoop.fs.Path;

import baidu.openresearch.kw.Constants;
import nju.lamda.hadoop.UtilsFileSystem;

public class SplitLabelKeyword 
{
	public static void run(String kwcls, String label, String kw) throws IOException
	{
		BufferedReader r = UtilsFileSystem.GetReader(
				new Path(kwcls));
		BufferedWriter wl = UtilsFileSystem.GetWriter(
				new Path(label));
		BufferedWriter wk = UtilsFileSystem.GetWriter(
				new Path(kw));
		
		long cnt = 1;
		String line;
		while((line = r.readLine())!=null)
		{
			line = line.trim();
			String parts[] = line.split("\t");
			String keyw,strLabel;
			if ( parts.length == 1)
			{
				keyw = "";
				strLabel = parts[0];
			}
			else
			{
				keyw = parts[0];
				strLabel = parts[1];
			}
			
			int ll = -999;
			if ( Character.isDigit(strLabel.charAt(0)))
			{
				ll = Integer.parseInt(strLabel);
			}
			
			wl.write(""+cnt+"\t"+ll+"\n");
			wk.write(""+cnt+"\t"+keyw+"\n");
			
			cnt ++;
		}
		
		r.close();
		wl.close();
		wk.close();
	}
	
	public static void main(String args[]) throws IOException
	{
		run(Constants.FILE_KEYWORD_CLASS, args[0],args[1]);
	}
}
