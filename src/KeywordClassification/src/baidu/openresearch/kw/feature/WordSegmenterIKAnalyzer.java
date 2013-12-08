package baidu.openresearch.kw.feature;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class WordSegmenterIKAnalyzer implements WordSegmenterBase {

	@Override
	public String[] segment(String line) throws IOException {
		if ( line.length() == 0)
		{
			return null;
		}
        StringReader sreader = new StringReader(line);
        IKSegmenter seg = new IKSegmenter(sreader, true);
        Lexeme lexeme;
        ArrayList<String> ret = new ArrayList<String>();
        
        while( (lexeme = seg.next())!=null)
        {
        	ret.add(lexeme.getLexemeText());
        }
        
        return ret.toArray(new String[0]);		
	}

}
