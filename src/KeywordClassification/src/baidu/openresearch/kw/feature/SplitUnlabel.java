package baidu.openresearch.kw.feature;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SplitUnlabel 
{
	public static void run(String datafile, String labelfile, String unlabelfile) throws IOException
	{
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		
		BufferedReader rdata = new BufferedReader(
				new InputStreamReader(fs.open(new Path(datafile))));
		BufferedWriter rlabel = new BufferedWriter(
				new OutputStreamWriter(fs.create(new Path(labelfile))));
		BufferedWriter rUnlabel = new BufferedWriter(
				new OutputStreamWriter(fs.create(new Path(unlabelfile))));
		
		while(true)
		{
			String line = rdata.readLine();
			if ( line == null)
			{
				break;
			}
			line = line.trim();
			if ( line.length() > 0)
			{
				int tpos = line.indexOf('\t');
				int bpos = line.indexOf(' ', tpos);
				
				if (bpos < 0)
				{
					bpos = line.length();
				}
				
				int label = Integer.parseInt(line.substring(tpos+1,bpos));
				if ( label == -999)
				{
					rUnlabel.write(line);
					rUnlabel.write('\n');
				}
				else
				{
					rlabel.write(line);
					rlabel.write('\n');
				}
			}
		}
		
		rdata.close();
		rlabel.close();
		rUnlabel.close();
		fs.close();
	}
	
	public static void main(String args[]) throws IOException
	{
		run(args[0],args[1],args[2]);
	}
}
