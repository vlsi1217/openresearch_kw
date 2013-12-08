package nju.lamda.svm;

import java.io.BufferedReader;
import java.io.IOException;

import de.bwaldvogel.liblinear.FeatureNode;

public class SvmIo 
{
	public static SvmRecord ReadRecord(BufferedReader input, double bias, int ndim) throws IOException
	{
		String line = input.readLine();
		if ( line == null)
		{
			return null;
		}
		
		if ( line.length() == 0)
		{
			return new SvmRecord(0, new FeatureNode[0]);
		}
		
		return Parse(line,ndim,bias);
	}
	
	public static SvmRecord Parse(String line)
	{
		return Parse(line,0,0);
	}
	public static SvmRecord Parse(String line, int ndim, double bias)
	{
		String pairs[] = line.split("[\\t ]");
		
		int label = Integer.parseInt(pairs[0]);
		int flen = pairs.length -1;
		if ( bias > 0)
		{
			flen++;
		}
		FeatureNode feature[] = new FeatureNode[flen];
		
		int i;
		for(i=1;i<pairs.length;i++)
		{
			String pair[] = pairs[i].split(":");
			int index = Integer.parseInt(pair[0]);
			double value = Double.parseDouble(pair[1]);
			feature[i-1] = new FeatureNode(index,value);
		}
		
		if ( bias > 0)
		{
			feature[i-1] = new FeatureNode(ndim,bias);
		}
		return new SvmRecord(label,feature);		
	}
	public static String ToString(SvmRecord rec)
	{
		StringBuffer buf = new StringBuffer();
		buf.append(rec.label);
		for(FeatureNode node : rec.feature)
		{
			buf.append(' ');
			buf.append(node.index);
			buf.append(':');
			buf.append(node.value);
		}
		
		return buf.toString();
	}
	
	public static String ToString(FeatureNode x[])
	{
		StringBuffer buf = new StringBuffer();
		for(FeatureNode node : x)
		{
			buf.append(' ');
			buf.append(node.index);
			buf.append(':');
			buf.append(node.value);
		}
		
		return buf.toString().trim();
	}
	
	public static void main(String args[])
	{
	}
}
