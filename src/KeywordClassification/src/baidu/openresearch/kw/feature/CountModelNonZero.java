package baidu.openresearch.kw.feature;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.fs.Path;

import nju.lamda.hadoop.UtilsFileSystem;

public class CountModelNonZero 
{
	public static void main(String args[]) throws IOException
	{
		BufferedReader r = UtilsFileSystem.GetReader(new Path(args[0]));
		Scanner scanner = new Scanner(r);
		
		int numDim = scanner.nextInt();
		scanner.nextInt();
		double bias = scanner.nextDouble();
		
		int wdim = numDim;
		if ( bias > 0)
		{
			wdim++;
		}
		
		boolean nnz[] = new boolean[wdim];
		while(scanner.hasNext())
		{
			scanner.nextInt();//label;
			for( int i=0;i<wdim;i++)
			{
				if ( scanner.nextDouble() != 0)
				{
					nnz[i] = true;
				}
			}
		}
		
		int cnt = 0;
		for(int i=0;i<wdim;i++)
		{
			if ( nnz[i] == true)
			{
				cnt ++;
			}
		}
		
		scanner.close();
		r.close();
		
		System.out.println(cnt);
	}
}
