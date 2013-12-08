package nju.lamda.hadoop.liblinear;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import nju.lamda.svm.SvmReader;
import nju.lamda.svm.SvmRecord;

public class SvmReaderContext implements SvmReader {

	private Mapper<IntWritable, Text, Text, Text>.Context context;

	private SvmRecord record;
	
	private int ndim;
	private double bias;

	SvmReaderContext(Mapper<IntWritable, Text, Text, Text>.Context ct,
			int nd, double b)
			throws IOException, InterruptedException {
		context = ct;
		record = getRecordFromText(context.getCurrentValue());
		ndim = nd;
		bias = b;
	}

	@Override
	public SvmRecord next() throws IOException {
		SvmRecord ret = record;
		if (ret != null) {
			try {

				if (context.nextKeyValue()) {
					record = getRecordFromText(context.getCurrentValue());
				} else {
					record = null;
				}
			} catch (InterruptedException e) {
				throw new IOException(e.getMessage());
			}
		}
		return ret;
	}

	public SvmRecord getRecordFromText(Text val) {
		SvmRecordHadoop rec = SvmRecordHadoop.FromText(val, ndim, bias);
		return rec.rec;
	}

}
