cluster manager: local[n], YARN, ......
execution mode: client or cluster
execution tools: shell, IDE, Notebook, Spark Submit

demo(1)
- cluster: local
- mode: client mode
- tool: spark-shell
# in terminal:
...\spark3> bin\pyspark --master local[3] --driver-memory 2G # 3 threads and 2G memory for driver
# ...\spark3> bin\pyspark --help for more options

df = spark.read.json("test.json")
df.show()

# to see the Spark Context Web UI
# the UI is available only when the spark application is running
# in web browser:
localhost:4040/jobs
localhost:4040/executors/ -> storage memory: 912.3MB # above 2GB is for the overall JVM

demo(2)
- cluster: YARN
- mode: client mode
- tool: spark-shell, Notebook
# go to cloud cluster terminal:
pyspark --master yarn --driver-memory 1G --executor-memory 500M --num-executor 2 --executor -cores 1
# go to cloud notebook
# establish connection to spark 
spark version -> automatically establish and create driver and executor using default values
# at the beginning of each cell:
%pyspark # because the default language is scala not python

demo(3)
- cluster: YARN
- mode: cluster mode
- tool: spark-submit

# open cloud terminal, in the setting click upload file
# select target file from the local disk
# in the terminal:
spark-submit --master yarn --deploy-mode cluster pi.py 100
