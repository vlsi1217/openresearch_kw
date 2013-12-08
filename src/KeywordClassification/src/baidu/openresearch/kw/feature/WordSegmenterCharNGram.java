package baidu.openresearch.kw.feature;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import nju.lamda.hadoop.feaext.Punctuations;

public class WordSegmenterCharNGram implements WordSegmenterBase {

	public int ngram;
	
	public WordSegmenterCharNGram(int n)
	{
		this.ngram = n;
	}
	
	@Override
	public String[] segment(String line) throws IOException {
		ArrayList<String> ret = new ArrayList<String>();
		
		for(int i=0;i<line.length();i++)
		{
			int j;
			for(j=0;(j<ngram)&&((i+j)<line.length());j++)
			{
				if ( Punctuations.IsPunctuation(""+line.charAt(i+j)))
				{
					break;
				}
			}
			if ( j==0)
			{
				continue;
			}
			
			String str = line.substring(i,i+j);
			for( j=str.length();j>0;j--)
			{
				ret.add(str.substring(0, j));
			}
		}
		
		return ret.toArray(new String[0]);
	}
	
	public static void main(String args[]) throws IOException
	{
		String x = "fdsfa fdsaffdaf.rewr";
		String w[] = (new WordSegmenterCharNGram(4)).segment(x);
		
		System.out.println(Arrays.toString(w));
	}

}
