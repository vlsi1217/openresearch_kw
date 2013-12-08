package nju.lamda.svm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class SvmReaderFile implements SvmReader 
{
	private int _ndim;
	private double _bias;
	private BufferedReader _reader;
	
	public SvmReaderFile( String filename, int ndim, double bias) throws FileNotFoundException
	{
		_ndim = ndim;
		_bias = bias;
		
		_reader = new BufferedReader(new FileReader(filename));
	}
	
	public SvmRecord next() throws IOException
	{
		if ( _reader == null)
		{
			return null;
		}
		else
		{
			SvmRecord rec = SvmIo.ReadRecord(_reader, _bias, _ndim);
			if ( rec == null)
			{
				_reader.close();
				_reader = null;
			}
			
			return rec;
		}
	}
}
