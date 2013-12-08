团队名称：HadoopEagleEye
作品名称：关键词分类
队员：邹晓川,陈庆国

运行说明：
    bin目录下有一份打包好的jar，运行程序:
    hadoop jar kw.jar baidu.openresearch.kw.Script /share/data/compad/keyword_class.txt result.txt
    hadoop fs -get result.txt .
    evaluate -kw result.txt
    整个可能会持续2到3小时，具体是环境而定。
    
代码说明：
    src目录下是代码，eclipse可直接导入。
    3rdparty目录下是需要的第三方库，需要导入eclipse工程。
