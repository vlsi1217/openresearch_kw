package baidu.openresearch.kw.feature;

import java.io.IOException;

public interface WordSegmenterBase {
	public abstract String[] segment(String line) throws IOException;

}
