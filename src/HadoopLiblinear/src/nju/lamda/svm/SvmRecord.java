package nju.lamda.svm;

import de.bwaldvogel.liblinear.FeatureNode;

public class SvmRecord 
{
	public int label;
	public FeatureNode feature[];
	
	public SvmRecord(int l, FeatureNode f[])
	{
		this.label = l;
		this.feature = f;
	}
	
	@Override
	public String toString()
	{
		return SvmIo.ToString(this);
	}
}
