package nju.lamda.svm;

import java.io.IOException;

public interface SvmReader 
{
	public abstract SvmRecord next() throws IOException;
}
