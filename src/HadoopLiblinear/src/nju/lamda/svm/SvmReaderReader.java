package nju.lamda.svm;

import java.io.BufferedReader;
import java.io.IOException;

public class SvmReaderReader implements SvmReader 
{
	private int _ndim;
	private double _bias;
	private BufferedReader _reader;
	
	public SvmReaderReader( BufferedReader reader, int ndim, double bias)
	{
		_ndim = ndim;
		_bias = bias;
		
		_reader = reader;	
	}	
	
	public SvmRecord next() throws IOException
	{
		return SvmIo.ReadRecord(_reader, _bias, _ndim);
	}
}
