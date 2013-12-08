package nju.lamda.svm.stream.linear;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.logging.Logger;

import nju.lamda.common.ArrayOp;
import nju.lamda.svm.SvmOp;
import nju.lamda.svm.SvmParam;
import nju.lamda.svm.SvmReader;
import nju.lamda.svm.SvmRecord;


import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.FeatureNode;
import de.bwaldvogel.liblinear.SolverType;

public class SvcDual 
{
	private final int MAX_ITER = 100;
	private Logger logger;
	private Random random;
	
	public void setLogger(Logger l)
	{
		logger = l;
	}
	
	public double[] solve(SvmReader reader, Parameter param) throws IOException
	{
		return solve(reader,param.ndim,param.targetLabel,param.bufferSize,
				param.minUpdateNum, param.svmParam);
	}
	public double[] solve(SvmReader reader,int ndim
			, int targetLabel, int sizeBuffer, int minUpdateNum
			, SvmParam param) throws IOException
	{
		random = new Random(0);
		double Cn,Cp;
		Cn = Cp = param.c;
		
        double diag[] = new double[] {0.5 / Cn, 0, 0.5 / Cp};
        double upper_bound[] = new double[] {Double.POSITIVE_INFINITY, 0, Double.POSITIVE_INFINITY};
        if (param.solverType == SolverType.L2R_L1LOSS_SVC_DUAL) {
            diag[0] = 0;
            diag[2] = 0;
            upper_bound[0] = Cn;
            upper_bound[2] = Cp;
        }
        
        FeatureNode X[][] = new FeatureNode[sizeBuffer][];
        int y[] = new int[sizeBuffer];
        
        double w[];
        if ( param.bias > 0)
        {
        	w = new double [ ndim+1 ];
        }
        else
        {
        	w = new double [ ndim ];
        }
        Arrays.fill(w, 0.0);
        double scores[] = new double [ sizeBuffer];
        Arrays.fill(scores, 2.0);
        double QD[] = new double [ sizeBuffer];
        Arrays.fill(QD, 0.0);
        double alpha[] = new double [ sizeBuffer];
        Arrays.fill(alpha, 0.0);
        
        int i=1;
		while(true)
		{
            int num = fillData(reader,X,y,w, scores, QD,diag,alpha,targetLabel, minUpdateNum, param.bias, ndim);
            
            if ( num == 0)
            {
            	break;
            }
            
            int innerIter = 0;
            if ( (i==1) && (num < sizeBuffer))
            {
            	innerIter = solveSingle(y,X,num,w,alpha,upper_bound,diag,QD,param.eps, scores);
            }
            else
            {
            	innerIter = solveSingle(y,X,sizeBuffer,w,alpha,upper_bound,diag,QD,param.eps,scores);
            }
            
            if ( logger != null)
            {
	            logger.info("iter:" + i + ",update_num:" + num + ",innter_iter:"
	            		+ innerIter);
            }
            System.gc();
            i++;
		}
		
		return w;
	}
	
	public int fillData(SvmReader reader, FeatureNode X[][], int y[], double w[], 
			double scores[], double QD[], double diag[], double alpha[]
					, int targetLabel, int minUpdateNum, double bias, int ndim) throws IOException
	{
		for( int i=0;i<X.length;i++)
		{
			if (X[i] != null)
			{
				scores[i] = SvmOp.dot(w, X[i])* y[i] -1;
			}
		}
		int cnt = 0;
		for(int i=0;i<X.length;i++)
		{
			if ( scores[i] > 0)
			{//not support vector
				SvmRecord rec = getNextRecord(reader,w,targetLabel,bias,ndim);
				if ( rec == null)
				{
					return cnt;
				}
				X[i] = rec.feature;
				y[i] = rec.label;
	            QD[i] = diag[rec.label+1];
	            alpha[i] = 0;
	            QD[i] += SvmOp.normL2Square(rec.feature);
	            scores[i] = 0;
	            cnt++;
			}
		}
		
		if ( cnt < minUpdateNum)
		{
			int k = minUpdateNum - cnt;
			int topkIndex[] = ArrayOp.TopKIndex(scores, k, true);
			for( int i=0;i<topkIndex.length;i++)
			{
				int idx = topkIndex[i];
				SvmRecord rec = getNextRecord(reader,w,targetLabel,bias, ndim);
				if ( rec == null)
				{
					return cnt;
				}
				X[idx] = rec.feature;
				y[idx] = rec.label;
	            QD[idx] = diag[rec.label+1];
	            alpha[idx] = 0;
	            QD[idx] += SvmOp.normL2Square(rec.feature);
	            scores[idx] = 0;
	            
	            cnt ++;
			}
		}
		
		return cnt;
	}
	
	public SvmRecord getNextRecord(SvmReader reader, double w[]
			,int targetLabel, double bias, int ndim) throws IOException
	{
		while( true)
		{
			SvmRecord rec = reader.next();
			if ( rec == null)
			{
				return null;
			}
			if ( targetLabel == rec.label)
			{
				rec.label = 1;
			}
			else
			{
				rec.label = -1;
			}
//			double score = rec.label * SvmOp.dot(w, rec.feature);
//			if ( score < 1)
//			{
//				return rec;
//			}
			return rec;
		}
	}
	public int solveSingle(int y[], FeatureNode X[][],int num, double w[]
			, double alpha[], double upper_bound[], double diag[], double QD[]
			, double eps, double scores[])
	{
		int iter = 1;
		double G,PG,C;
		int yi = 0;
		FeatureNode Xi[];
		double alphai;
		double QDi;
		double d;
	
		double PGmax_old,PGmin_old,PGmax_new,PGmin_new;
		
        PGmax_old = Double.POSITIVE_INFINITY;
        PGmin_old = Double.NEGATIVE_INFINITY;
        
        int active_size = num;
        int index[] = new int[num];
        for(int i=0;i<num;i++)
        {
        	index[i] = i;
        }
        
        for (int i = 0; i < active_size; i++) {
            int j = i + random.nextInt(active_size - i);
            int tmp = index[i];
            index[i] = index[j];
            index[j] = tmp;
        }        
        
		while( iter < MAX_ITER)
		{
            PGmax_new = Double.NEGATIVE_INFINITY;
            PGmin_new = Double.POSITIVE_INFINITY;
            
			for(int i=0;i<active_size;i++)
			{
				int idx = index[i];
				yi = y[idx];
				Xi = X[idx];
				alphai = alpha[idx];
				QDi = QD[idx];
				G = SvmOp.dot(w, Xi);
				G = G * yi - 1;
				//scores[i] = G;
                C = upper_bound[yi+1];
                G += alphai * diag[yi+1];

                PG = 0;
                if (alphai == 0) 
                {
                    if (G > PGmax_old) 
                    {
                        continue;
                    } 
                    else if (G < 0) 
                    {
                    	PG = G;
                    }
                } 
                else if (alphai == C) 
                {
                    if (G < PGmin_old) 
                    {
                        continue;
                    } 
                    else if (G > 0) 
                    {
                        PG = G;
                    }
                } 
                else 
                {
                    PG = G;
                }
                
                PGmax_new = Math.max(PGmax_new, PG);
                PGmin_new = Math.min(PGmin_new, PG);
                
                if (Math.abs(PG) > 1.0e-12) 
                {
                    alpha[idx] = Math.min(Math.max(alphai - G / QDi, 0.0), C);
                    d = (alpha[idx] - alphai) * yi;

                    for (Feature xi : Xi) 
                    {
                        w[xi.getIndex() - 1] += d * xi.getValue();
                    }
                }	
			}//for
			
            if (PGmax_new - PGmin_new <= eps) 
            {
            	break;
            }
            PGmax_old = PGmax_new;
            PGmin_old = PGmin_new;					
			iter++;
		}//while
		
		return iter;
	}
}
