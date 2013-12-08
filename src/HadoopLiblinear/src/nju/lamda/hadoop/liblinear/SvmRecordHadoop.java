package nju.lamda.hadoop.liblinear;

import nju.lamda.svm.SvmIo;
import nju.lamda.svm.SvmRecord;

import org.apache.hadoop.io.Text;

public class SvmRecordHadoop 
{
	public String id;
	public SvmRecord rec;
	
	public SvmRecordHadoop(String i, SvmRecord r)
	{
		id = i;
		rec = r;
	}
	
	public static SvmRecordHadoop FromText(Text text)
	{
		return FromText(text,0,0);
	}
	
	public static SvmRecordHadoop FromText(Text text, int ndim, double bias)
	{
		String line = text.toString();
		int bpos = line.indexOf('\t');
		String id = line.substring(0, bpos);
		String subline = line.substring(bpos+1);
		SvmRecord rec = SvmIo.Parse(subline, ndim, bias);
		
		return new SvmRecordHadoop(id,rec);		
	}
	
	public static SvmRecordHadoop Parse(String line, int ndim, double bias)
	{
		int bpos = line.indexOf('\t');
		String id = line.substring(0, bpos);
		String subline = line.substring(bpos+1);
		SvmRecord rec = SvmIo.Parse(subline, ndim, bias);
		
		return new SvmRecordHadoop(id,rec);			
	}
}
