package nju.lamda.svm.stream.linear;

import de.bwaldvogel.liblinear.SolverType;
import nju.lamda.svm.SvmParam;

public class Parameter 
{
	public SvmParam svmParam;
	public String dataFilename;
	public String modelFilename;
	public int bufferSize;
	public int minUpdateNum;
	public int targetLabel;
	public int ndim;
	
	public Parameter()
	{
		svmParam =  new SvmParam(SolverType.L2R_L2LOSS_SVC_DUAL,
				1.0, 0.1, 1.0);
	}
	
	public int parseCommand(String argv[])
	{
		int i;
		
        for (i = 0; i < argv.length; i++) {
            if (argv[i].charAt(0) != '-') break;
            if (++i >= argv.length) {
            	throw new IllegalArgumentException();
            }
            
            switch (argv[i - 1].charAt(1)) {
                case 's':
                    svmParam.solverType = SolverType.getById(Integer.parseInt(argv[i]));
                    break;
                case 'c':
                    svmParam.c = Double.parseDouble(argv[i]);
                    break;
                case 'e':
                    svmParam.eps = Double.parseDouble(argv[i]);
                    break;
                case 'B':
                    svmParam.bias = Double.parseDouble(argv[i]);
                    break;
                case 't':
                	targetLabel = Integer.parseInt(argv[i]);
                	break;
                case 'u':
                	bufferSize = Integer.parseInt(argv[i]);
                	break;
                case 'm':
                	minUpdateNum = Integer.parseInt(argv[i]);
                	break;
                case 'd':
                	ndim = Integer.parseInt(argv[i]);
                	break;
                	
                default:
                    System.err.println("unknown option");
                    throw new IllegalArgumentException();
            }
        }
        
        if (i >= argv.length) throw new IllegalArgumentException();

        dataFilename = argv[i];

        if (i < argv.length - 1)
        {
            modelFilename = argv[i + 1];
            i++;
        }
        else {
            int p = argv[i].lastIndexOf('/');
            ++p; // whew...
            modelFilename = argv[i].substring(p) + ".model";
        }
		
        return i;
	}
	
	public void report()
	{
		System.out.println("Stream SVM Parameter:");
		System.out.println("data filename: " + this.dataFilename);
		System.out.println("model filename: " + this.modelFilename);
		System.out.println("buffer size: " + this.bufferSize);
		System.out.println("minimal update: " + this.minUpdateNum);
		System.out.println("target label: " + this.targetLabel);
		System.out.println("dimension: " + this.ndim);
		System.out.println("svm.c: " + this.svmParam.c);
		System.out.println("svm.bias: " + this.svmParam.bias);
		System.out.println("svm.eps: " + this.svmParam.eps);
	}
}
