package nju.lamda.hadoop.liblinear;

import nju.lamda.hadoop.UtilsFileSystem;
import nju.lamda.svm.stream.linear.Parameter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Train 
{
	public static void main(String args[]) throws Exception
	{
		Parameter param = new Parameter();
		int i = param.parseCommand(args);
		
		i++;
		String workdir = args[i++];
		
		(new Train()).run(param.dataFilename, param.modelFilename
				, workdir, param.svmParam.c, param.svmParam.bias, 
				param.svmParam.eps, param.bufferSize, param.minUpdateNum);
	}
	public int run(String datafile, String modelfile, String workingDir,
			double c, double bias, double eps, int buffersize, int minupdate) 
			throws Exception
	{
		TrainPreproc preProc = new TrainPreproc();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		String labelfile = workingDir + "/" + Constants.FILE_SVM_LABEL;
		
		UtilsFileSystem.ForceDir(fs, new Path(workingDir));
		int retcd = preProc.run(datafile, labelfile,
				 workingDir);
		if ( retcd != 0)
		{
			return 1;
		}
		
		int ndim = preProc.maxIndex;
		int nclass = preProc.numClass;
		
		System.out.println("--------------");
		System.out.println("number of dim: " + ndim);
		System.out.println("number of class: " + nclass);
		System.out.println("--------------");
		
		TrainProc proc = new TrainProc();
		
		retcd = proc.run(datafile, labelfile, modelfile, c, bias, eps, ndim
				, buffersize, minupdate, nclass, workingDir);
		
		return retcd;
	}
}
