package baidu.openresearch.kw.feature;

import java.io.IOException;
import java.util.ArrayList;

import nju.lamda.hadoop.feaext.Punctuations;

public class WordSegmenterChar implements WordSegmenterBase {

	@Override
	public String[] segment(String line) throws IOException {
		ArrayList<String> ret = new ArrayList<String>();
		
		for(int i=0;i<line.length();i++)
		{
			char ch = line.charAt(i);
			String str = "" + ch;
			if ( !Punctuations.IsPunctuation(str))
			{
				ret.add(str);
			}
		}
		
		return ret.toArray(new String[0]);
	}

}
