package nju.lamda.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class CopyFileSplit extends InputSplit implements Writable
{
	private int _index;
	private long _length;
	private Path _path;
	
	public CopyFileSplit()
	{
		
	}
	public CopyFileSplit(Path path, long len, int index)
	{
		this._path = path;
		this._length = len;
		this._index = index;
	}
	
    public long getLength() throws IOException, InterruptedException
    {
    	return this._length;
    }

    public Path getPath()
    {
    	return _path;
    }
    
    public int getIndex()
    {
    	return _index;
    }
    
    public String[] getLocations() throws IOException
    , InterruptedException
    {
    	return new String[0];
    }

	@Override
	public void readFields(DataInput datainput) throws IOException {
		_index = datainput.readInt();
		_length = datainput.readLong();
		_path = new Path(Text.readString(datainput));
		
	}

	@Override
	public void write(DataOutput dataoutput) throws IOException {
		// TODO Auto-generated method stub
		dataoutput.writeInt(_index);
		dataoutput.writeLong(_length);
		Text.writeString(dataoutput, _path.toString());
	}
    		
}
