package nju.lamda.svm.stream.linear;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Logger;

import nju.lamda.svm.SvmReaderReader;

import de.bwaldvogel.liblinear.SolverType;

public class BinaryTrain 
{
	public static void main(String args[]) throws IOException
	{
		Parameter param = ParseCommand(args);
		
		BufferedReader reader = new BufferedReader(new FileReader(param.dataFilename));
		SvmReaderReader svmReader = new SvmReaderReader(reader,
				param.ndim, param.svmParam.bias);
		Logger logger = Logger.getLogger("BinaryTrain");
		
		double w[] = new double[0];
		
		if ( (param.svmParam.solverType == SolverType.L2R_L1LOSS_SVC_DUAL)
				|| (param.svmParam.solverType == SolverType.L2R_L2LOSS_SVC_DUAL))
		{
			SvcDual solver = new SvcDual();
			solver.setLogger(logger);
			
			long s = System.currentTimeMillis();
			w = solver.solve(svmReader, param);
			logger.info("time cost: " + (System.currentTimeMillis()-s));
		}
		
		Model m = new Model();
		m.bias = param.svmParam.bias;
		m.nr_class = 2;
		m.label = new int[2];
		m.label[0] = 1;
		m.label[1] = -1;
		m.nr_feature = param.ndim;
		m.solverType = param.svmParam.solverType;
		m.w = w;
		m.save(param.modelFilename);
	}
	
	public static void exit_with_help()
	{
        System.out.printf("Usage: train [options] training_set_file [model_file]%n" //
                + "options:%n"
                + "-s type : set type of solver (default 1)%n"
                + "  for multi-class classification%n"
                + "    0 -- L2-regularized logistic regression (primal)%n"
                + "    1 -- L2-regularized L2-loss support vector classification (dual)%n"
                + "    2 -- L2-regularized L2-loss support vector classification (primal)%n"
                + "    3 -- L2-regularized L1-loss support vector classification (dual)%n"
                + "    4 -- support vector classification by Crammer and Singer%n"
                + "    5 -- L1-regularized L2-loss support vector classification%n"
                + "    6 -- L1-regularized logistic regression%n"
                + "    7 -- L2-regularized logistic regression (dual)%n"
                + "  for regression%n"
                + "   11 -- L2-regularized L2-loss support vector regression (primal)%n"
                + "   12 -- L2-regularized L2-loss support vector regression (dual)%n"
                + "   13 -- L2-regularized L1-loss support vector regression (dual)%n"
                + "-c cost : set the parameter C (default 1)%n"
                + "-p epsilon : set the epsilon in loss function of SVR (default 0.1)%n"
                + "-e epsilon : set tolerance of termination criterion%n"
                + "   -s 0 and 2%n" + "       |f'(w)|_2 <= eps*min(pos,neg)/l*|f'(w0)|_2,%n"
                + "       where f is the primal function and pos/neg are # of%n"
                + "       positive/negative data (default 0.01)%n" + "   -s 11%n"
                + "       |f'(w)|_2 <= eps*|f'(w0)|_2 (default 0.001)%n"
                + "   -s 1, 3, 4 and 7%n" + "       Dual maximal violation <= eps; similar to libsvm (default 0.1)%n"
                + "   -s 5 and 6%n"
                + "       |f'(w)|_1 <= eps*min(pos,neg)/l*|f'(w0)|_1,%n"
                + "       where f is the primal function (default 0.01)%n"
                + "   -s 12 and 13\n"
                + "       |f'(alpha)|_1 <= eps |f'(alpha0)|,\n"
                + "       where f is the dual function (default 0.1)\n"
                + "-B bias : if bias >= 0, instance x becomes [x; bias]; if < 0, no bias term added (default -1)%n"
                );
            System.exit(1);		
	}
	public static Parameter ParseCommand(String argv[])
	{
		int i;
		
		Parameter param = new Parameter();
		
        for (i = 0; i < argv.length; i++) {
            if (argv[i].charAt(0) != '-') break;
            if (++i >= argv.length) exit_with_help();
            switch (argv[i - 1].charAt(1)) {
                case 's':
                    param.svmParam.solverType = SolverType.getById(Integer.parseInt(argv[i]));
                    break;
                case 'c':
                    param.svmParam.c = Double.parseDouble(argv[i]);
                    break;
                case 'e':
                    param.svmParam.eps = Double.parseDouble(argv[i]);
                    break;
                case 'B':
                    param.svmParam.bias = Double.parseDouble(argv[i]);
                    break;
                case 't':
                	param.targetLabel = Integer.parseInt(argv[i]);
                	break;
                case 'u':
                	param.bufferSize = Integer.parseInt(argv[i]);
                	break;
                case 'm':
                	param.minUpdateNum = Integer.parseInt(argv[i]);
                	break;
                case 'd':
                	param.ndim = Integer.parseInt(argv[i]);
                	break;
                	
                default:
                    System.err.println("unknown option");
                    exit_with_help();
            }
        }
        
        if (i >= argv.length) exit_with_help();

        param.dataFilename = argv[i];

        if (i < argv.length - 1)
        {
            param.modelFilename = argv[i + 1];
        }
        else {
            int p = argv[i].lastIndexOf('/');
            ++p; // whew...
            param.modelFilename = argv[i].substring(p) + ".model";
        }
		
        return param;
	}
}
