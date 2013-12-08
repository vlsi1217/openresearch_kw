package nju.lamda.hadoop.liblinear;

public class Predict 
{
	public static void main(String args[]) throws NumberFormatException, Exception
	{
//		(new Predict()).run(args[0], args[1], args[2], args[4]
//				, Integer.parseInt(args[3]));
		(new Predict()).run(args[0], args[1], args[2], args[3]);
	}
//	public int run(String datafile, String modelfile, String resultfile,
//			String workingDir, int buffersize) throws Exception
//	{
//		PredictProc proc = new PredictProc();
//		return proc.run(datafile, modelfile, resultfile, workingDir, buffersize);
//	}
	
	public int run(String datafile, String modelfile, String resultfile,
			String workdir) throws Exception
	{
		PredictProcEx proc = new PredictProcEx();
		return proc.run(datafile,modelfile,resultfile,workdir);
	}
	
}
