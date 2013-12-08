package nju.lamda.hadoop.feaext;

import org.apache.hadoop.fs.Path;

import nju.lamda.hadoop.UtilsFileSystem;

public class TfIdf {
	public int run(String filetext,String filedict, String fileresult
			,boolean binary, int mindf, String workdir) throws Exception
	{
		GenerateDictProc genDict = new GenerateDictProc();
		
		int retcd = 0;
		if ( !UtilsFileSystem.Exists(new Path(filedict)))
		{
			System.out.println("dict file dose not exist");
			retcd = genDict.run(filetext, filedict, mindf, workdir);
			if ( retcd != 0)
			{
				return retcd;
			}
		}
		else
		{
			System.out.println("dict file exist");
		}
		
		VectorizeProc1 proc1 = new VectorizeProc1();
		VectorizeProc2 proc2 = new VectorizeProc2();
		
		retcd = proc1.run(filetext, filedict, binary, workdir);
		if ( retcd!=0)
		{
			return retcd;
		}
		retcd = proc2.run(fileresult, workdir);
		
		return retcd;
	}

	public static void main(String args[]) throws Exception
	{
		boolean bin = args[3].equals("0");
		int mindf = Integer.parseInt(args[4]);
		(new TfIdf()).run(args[0], args[1], args[2], bin, mindf, args[5]);
	}
}
