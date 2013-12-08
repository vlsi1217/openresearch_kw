package nju.lamda.svm;

import de.bwaldvogel.liblinear.SolverType;

public class SvmParam 
{
	public SolverType solverType; 
	public double c;
	public double eps;
	public double bias;
	
	public SvmParam(SolverType st, double cc, double epss, double biass)
	{
		this.solverType = st;
		this.c = cc;
		this.eps = epss;
		this.bias = biass;
	}
}
