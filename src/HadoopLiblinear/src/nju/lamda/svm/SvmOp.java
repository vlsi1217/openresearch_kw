package nju.lamda.svm;

import de.bwaldvogel.liblinear.Feature;

public class SvmOp 
{
	public static double dot(double w[], Feature x[])
	{
		double sum = 0.0;
		
		for( Feature xi : x)
		{
			int index = xi.getIndex()-1;
			if ( index >= w.length )
			{
				break;
			}
			sum += w[index] * xi.getValue();
		}
		
		return sum;
	}
	
	public static double normL2Square(Feature x[])
	{
		double norm = 0.0;
		double val;
		for(Feature f : x)
		{
			val = f.getValue();
			norm += val * val;
		}
		return norm;
	}
	
	public static void normalizeL2(Feature x[])
	{
		double norm = normL2Square(x);
		norm = Math.sqrt(norm);
		
		for(Feature f: x)
		{
			f.setValue(f.getValue()/norm);
		}
	}
}
