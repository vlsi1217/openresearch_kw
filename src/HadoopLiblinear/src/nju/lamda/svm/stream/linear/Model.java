package nju.lamda.svm.stream.linear;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Formatter;
import java.util.Locale;

import de.bwaldvogel.liblinear.SolverType;

public class Model 
{
    public double                    bias;

    /** label of each class */
    public int[]                     label;

    public int                       nr_class;

    public int                       nr_feature;

    public SolverType                solverType;

    /** feature weight array */
    public double[]                  w;
    
    public void save(String filename) throws IOException
    {
        int w_size = nr_feature;
        if (bias > 0) w_size++;

        int nr_w = nr_class;
        if (nr_class == 2 && solverType != SolverType.MCSVM_CS) nr_w = 1;

        BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
        Formatter formatter = new Formatter(writer, Locale.ENGLISH);
        try {
            formatter.format("solver_type %s\n", solverType.name());
            formatter.format("nr_class %d\n", nr_class);

            if (label != null) {
                formatter.format("label");
                for (int i = 0; i < nr_class; i++) {
                    formatter.format(" %d", label[i]);
                }
                formatter.format("\n");
            }

            formatter.format("nr_feature %d\n", nr_feature);
            formatter.format("bias %.16g\n", bias);

            formatter.format("w\n");
            for (int i = 0; i < w_size; i++) {
                for (int j = 0; j < nr_w; j++) {
                    double value = w[i * nr_w + j];

                    /** this optimization is the reason for {@link Model#equals(double[], double[])} */
                    if (value == 0.0) {
                        formatter.format("%d ", 0);
                    } else {
                        formatter.format("%.16g ", value);
                    }
                }
                formatter.format("\n");
            }

            formatter.flush();
            IOException ioException = formatter.ioException();
            if (ioException != null) throw ioException;
        }
        finally {
            formatter.close();
        }    	
    }
    
    public void load(String filename)
    {
    	
    }
	
}
