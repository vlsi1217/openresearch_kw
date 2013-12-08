package nju.lamda.hadoop.liblinear;

public class Constants 
{
	public static final String TMP_ROOT = "svmtmp";
	
	public static final String OUTDIR_SVM_PRETRAIN = 
			TMP_ROOT+"/"+"output_svmpretrain";
	public static final String OUTDIR_SVM_TRAIN = TMP_ROOT+"/"+"output_svmtrain";
	public static final String OUTDIR_SVM_PREDICT = 
			TMP_ROOT+"/"+"output_svmpredict";
	public static final String FILE_SVM_LABEL = TMP_ROOT+"/"+"label.txt";
	public static final String TMP_FILE_TRAIN = TMP_ROOT+"/"+"tmp_train";
	public static final String TMP_FILE_TEST = TMP_ROOT+"/"+"tmp_test";
	public static final String TMP_FILE_MODEL = TMP_ROOT+"/"+"tmp_model";
	public static final String TMP_FILE_RESULT = TMP_ROOT+"/"+"tmp_result";

}
