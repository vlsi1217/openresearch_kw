package nju.lamda.hadoop.liblinear;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class SplitTrainTest{
	
	public static void RandomSplit(String datafile, String trainfile, String testfile,
			int fold) throws IOException{
		// TODO Auto-generated method stub
		System.out.println("begin radnom split...");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		BufferedReader rData = new BufferedReader(
				new InputStreamReader(fs.open(new Path(datafile))));
		
		BufferedWriter rTrain = new BufferedWriter(
				new OutputStreamWriter(fs.create(new Path(trainfile))));
		
		BufferedWriter rTest = new BufferedWriter(
				new OutputStreamWriter(fs.create(new Path(testfile))));
		
		Random rand = new Random();
		while(true)
		{
			String line = rData.readLine();
			if ( line == null)
			{
				break;
			}
			line = line.trim();
			if ( line.length()>0)
			{
				if ( rand.nextInt(fold) == 0)
				{//test
					rTest.write(line);
					rTest.write('\n');
				}
				else
				{//train
					rTrain.write(line);
					rTrain.write('\n');
				}
				
			}
		}
		
		rData.close();
		rTrain.close();
		rTest.close();
		fs.close();
		System.out.println("end radnom split");
	}
}
