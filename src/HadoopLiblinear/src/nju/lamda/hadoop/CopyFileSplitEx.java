package nju.lamda.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CopyFileSplitEx extends InputSplit implements Writable{
	private FileSplit _fileSplit;
	private int _index;

	public CopyFileSplitEx() {
		_fileSplit = new FileSplit(null,0,0,null);
	}

	public CopyFileSplitEx(FileSplit fsp, int index) {
		this._fileSplit = fsp;
		this._index = index;
	}

	public Path getPath() {
		return _fileSplit.getPath();
	}

	public FileSplit getFileSplit()
	{
		return _fileSplit;
	}
	public int getIndex()
	{
		return _index;
	}
	
	public long getStart() {
		return _fileSplit.getStart();
	}

	@Override
	public long getLength() {
		return _fileSplit.getLength();
	}

	public String toString() {
		return _fileSplit.toString()+","+_index;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		_fileSplit.write(out);
		out.writeInt(_index);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		_fileSplit.readFields(in);
		_index = in.readInt();
	}

	@Override
	public String[] getLocations() throws IOException {
		//return _fileSplit.getLocations();
		return new String[0];
	}

}