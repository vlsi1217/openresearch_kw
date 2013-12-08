package nju.lamda.hadoop.liblinear;

import nju.lamda.common.ArrayOp;
import nju.lamda.hadoop.UtilsFileSystem;
import nju.lamda.svm.stream.linear.Parameter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RandomValidate 
{
	public static double[][] run(String datafile, int fold, int nrun, 
			String workingdir, double c, double eps, double bias,
			int bufferSize, int minUpdate, double thresh) throws Exception
	{
		double acc[][] = new double[nrun][];
		
		String filenameTrain = workingdir + "/" + Constants.TMP_FILE_TRAIN;
		String filenameTest = workingdir + "/" + Constants.TMP_FILE_TEST;
		String filenameModel = workingdir + "/" + Constants.TMP_FILE_MODEL;
		String filenameResult = workingdir + "/" + Constants.TMP_FILE_RESULT;
		for(int i=0;i<nrun;i++)
		{
			SplitTrainTest.RandomSplit(datafile, filenameTrain
					, filenameTest, fold);
			int retcd = (new Train()).run(filenameTrain
					, filenameModel, workingdir, c, bias, eps
					, bufferSize, minUpdate);
			if ( retcd != 0)
			{
				return null;
			}
			
			(new Predict()).run(filenameTest, filenameModel
					, filenameResult, workingdir);//, bufferSize);
			acc[i] = CompareResult.Compare(filenameTest,filenameResult, thresh);
		}
		
		return acc;
	}
	
	
	public static void main(String args[]) throws Exception
	{
		System.out.println("begin radnom validate");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		UtilsFileSystem.ForceDir(fs, new Path(Constants.TMP_ROOT));
		
		Parameter param = new Parameter();
		int i = param.parseCommand(args);
		
		int fold = Integer.parseInt(args[i++]);
		int nrun = Integer.parseInt(args[i++]);
		double thresh = Double.parseDouble(args[i++]);
		
		param.report();
		System.out.println("fold : " + fold);
		System.out.println("run: " + nrun);
		
		double acc[][] = run(param.dataFilename,fold,nrun,".",param.svmParam.c,
				param.svmParam.eps,param.svmParam.bias,param.bufferSize,
				param.minUpdateNum, thresh);
		
		System.out.println(ArrayOp.ToString(acc));
		
		System.out.println("end random validate");
	}
}
