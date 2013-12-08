package nju.lamda.hadoop.liblinear;

public class HadoopSvmIo 
{
	public static PairKeyLabel GetKeyLabel(String line)
	{
		int tpos = line.indexOf('\t');
		int bpos = line.indexOf(' ', tpos+1);
		if ( bpos < 0)
		{
			bpos = line.length();
		}
		
		String key = line.substring(0,tpos);
		int label = Integer.parseInt(line.substring(tpos+1,bpos));
		return new PairKeyLabel(key,label);
	}
}
