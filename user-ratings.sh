spark-submit --class als.SumRatings --master spark://soit-hdp-pro-1.ucc.usyd.edu.au:7077  --total-executor-cores 10 SparkML.jar hdfs://soit-hdp-pro-1.ucc.usyd.edu.au:8020/share/movie/small/  sumRatings.txt
