# from pyspark import  SparkContext
# sc = SparkContext("local","PySpark Word Count Exmaple")
	
# # read data from text file and split each line into words
# data = sc.textFile("C:/Users/argent/Desktop/desk/NYU 课程/2020 Summer/Big Data/spark/test.txt").flatMap(lambda line: line.split(" "))

# # count the occurrence of each word
# wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)

# wordCounts.saveAsTextFile("C:/Users/argent/Desktop/desk/NYU 课程/2020 Summer/Big Data/spark/wordcount/")



from pyspark import SparkConf, SparkContext
 
# 创建SparkConf和SparkContext
conf = SparkConf().setMaster("local").setAppName("lichao-wordcount")
sc = SparkContext(conf=conf)
 
# 输入的数据
# data=["hello","world","hello","word","count","count","hello"]
data = sc.textFile("C:/Users/argent/Desktop/desk/NYU 课程/2020 Summer/Big Data/spark/test.txt").flatMap(lambda line: line.split(" "))


 
# 将Collection的data转化为spark中的rdd并进行操作
rdd=sc.parallelize(data)
resultRdd = rdd.map(lambda word: (word,1)).reduceByKey(lambda a,b:a+b)
 
# rdd转为collecton并打印
resultColl = resultRdd.collect()
for line in resultColl:
    print(line)
 
# 结束
sc.stop()

