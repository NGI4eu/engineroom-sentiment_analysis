## Analyzing Posts

#List of keywords to analyze
words = ['microtargeting', 'recommender system', 'deletefacebook', 'digital surveillance','context awareness','data sovereignty', 'killer robot', 'computer-supported collaboration', 'digital learning', 'project maven', 'digital signature', 'cloud computing', 'vehicle-to-vehicle', 'internet governance', 'filter bubble', 'personal cloud', 'huggiebot', 'information privacy', 'wealth concentration', 'internet safety', 'data localization', 'artificial general intelligence', 'algorithms', 'distance education', 'sustainability', 'cybersecurity', 'stellar', 'gsm', 'computer vision', 'surveillance', 'e-administration', 'monopoly', 'autonomous car', 'cyber sovereignty', 'artificial intelligence', 'hyperconnectivity', 'anticipatory governance', 'innovation hub', 'digital divide', 'social objects', 'psychological effects of internet use', 'data analysis', 'industry 4.1', 'resilience', 'speech recognition', 'deep neural', 'encryption', 'personally identifiable information', 'monero', 'reinforcement learning', 'decentralisation', 'credential stuffing', 'quantum computing', 'homomorphic encryption', 'altright', 'metoo', 'electronic voting', 'two-sided markets', 'brain-computer interface', 'post-truth', 'open innovation', 'secure electronic transaction', '5g standard', 'educational technology', 'privaci policy','apple inc', 'blockchain platform', 'conspiracy theory', 'privacy by design', 'open-source software', 'quantum technology', 'data aggregation', 'cybercrime', 'digital health', 'network security','secure communication', 'smart objects', 'coinhiv', 'scalability', 'virtualization', 'freedom of speech', 'swarm intelligence', 'internet freedom', 'e-commerce', 'virtual private network', 'ai assistant', 'mass surveillance', 'open-source governance', 'privacy','digital twin', 'empowerment', 'right to internet access', 'trust federation','backward compatibility', 'distributed ledger', 'ehealth', 'infowar', 'information explosion','quantum supremacy', 'political polarization', 'virtual currency', 'ai algorithm', 'deepfake', "kerckhoffs's principle", 'youtube kids', 'v2v', 'zte', 'data broker', 'cryptography', 'multicloud', 'robot tax', 'robot consciousness', 'artificial neural network', 'copyright', 'digital labor', 'interoperability', 'data retention', 'open data', 'fake account','conscious computing','election meddling', 'person robot', 'chinese tech', 'factcheck', 'personal data','ethereum', 'privacy-enhancing technologies','data literacy','open platform', '5g', 'hyperledger', 'sharing economy', 'virtual reality', 'green computing', 'ubiquitous computing', 'user-centered design', 'live streaming', 'conspiracy theories', 'data ownership', 'black box', 'open-source model', 'google','data scandal', 'participatory democracy', 'fake news', 'economic inequality', 'usability', 'reputation system', 'speech translation', 'qubit', 'future proof', 'cyberattack','nanotechnology', 'open-source hardware', 'general data protection regulation', 'web accessibility', 'facebook', 'digital transformation', 'youtube kid', '3gpp', 'cambridge analytica', 'internet access', 'living lab', 'price discrimination', 'eye tracking', 'w3c', 'internet', 'data mining', 'facebook data', 'exascal', 'digital commons', 'echo chamber', 'augmented reality', 'braveheart effect', 'censorship', 'quantum network', 'secure by design', 'social innovation', 'tamper resistance', 'internet privacy', 'russian interference', 'open spectrum', 'automated reasoning', 'data visualization', 'notpetya', 'intellectual property','object recognition', 'global basic income', 'gps', 'decentralization', 'deep learning', 'linux', 'identity management', 'targeted advertising', 'nbn', 'human-computer interaction', 'machine learning', 'bci', 'fog computing', 'netneutrality', 'internet universality', 'russian troll', 'iot', 'network transparency', 'e-procurement', 'ai chip', 'open standard', 'cloud native', 'multidisciplinary approach', 'tencent', 'global education', 'open source', 'explainable artificial intelligence', 'open security', 'blockchain', 'kosinski', 'hate speech', 'image recognition', 'hci', 'data center', 'smart grid', 'algorithmic discrimination', 'peer-to-peer', 'disruptive innovation', 'real-time collaboration', 'ai system', 'right to be forgotten', 'globalization', 'digital single market', 'soft robotics', 'user interface', 'internet fraud', 'user privacy', 'evolutionary computing', 'financial technology', 'robotics', 'digital citizen', 'virtual community', 'temporary work', 'p2p', 'bitcoin', 'e-democracy', 'gdpr', 'circular economy', 'knowledge commons', 'recommender systems', 'semantic analysis', 'immersive technology', 'autonomous weapon', 'aidriven', 'information society', 'internet of things', 'smart contract', 'internet security', 'autonomous vehicle', 'social media', 'twitter', 'user experience', 'cryptocurrencies', 'semantic web', 'wannacri', 'digital identity', 'distributed computing', 'deep web', 'global positioning system', 'provable security', 'level playing field', 'social learning network', 'misinformation', 'digital economy', 'amazon', 'social inequality', 'social networking service', 'smartphone', 'machine translation', 'edge computing', 'personalized marketing', 'privacy policy', 'big data', 'vpn', 'project zero', 'algorithmic bias', 'internet backbone', 'competition law', 'openai', 'virtual collaboration', 'ncsc', 'world wide web consortium', 'ai startup', 'cryptocurrency', 'open access', 'algorithmic regulation', 'smart city', 'net neutrality', 'digital currency', 'green economy', 'information ethics', 'regulatory technology', 'robot ethics','dark web']

#Reading file (reddit POSTS)
df=spark.read.json("/user/data/submissions/").select(['id', 'num_comments', 'title', 'created_utc'])

# Define udf
from pyspark.sql.functions import udf

#Selecting the POSTS that contains one of the keywords. Adding the keyword as a column.
def findTitlesKeyword(title):
    for word in words:
        if word in title.lower():
            return word
    return 'a'
    
from pyspark.sql.types import StringType

udfFindTitlesKeyword=udf(findTitlesKeyword, StringType())
df_unfiltered = df.withColumn("keyword", udfFindTitlesKeyword("title"))

df_filtered = df_unfiltered.filter(df_unfiltered["keyword"]!="a")

#Saving selected POSTS
df_filtered.write.option("header", "true").csv("/user/data/submissions_processed_text")

df_filtered.show()
'''
+------+------------+--------------------+-----------+--------------------+
|    id|num_comments|               title|created_utc|             keyword|
+------+------------+--------------------+-----------+--------------------+
|8nncjk|           0|Personalized Mark...| 1527811200|personalized mark...|
|8nncml|           0|/r/linux salivate...| 1527811218|               linux|
|8nncnm|           0|NBA Finals : 'Cav...| 1527811224|      live streaming|
|8nncov|          36|Roseanne's Twitte...| 1527811231|             twitter|
|8nncqq|           0|Google Developer ...| 1527811245|              google|
|8nncra|           3|Report: Google, W...| 1527811250|              google|
|8nncs5|           0|Ripple CEO Compar...| 1527811254|             bitcoin|
|8nnct2|           0|Virtual Reality G...| 1527811259|     virtual reality|
|8nncvf|           3|180531 Korean Red...| 1527811277|            facebook|
|8nncz4|           3|Idiot in a Van in AZ| 1527811329|                 iot|
|8nnczl|           0|America’s Teens A...| 1527811306|            facebook|
|8nnczs|           5|The Internet is o...| 1527811307|            internet|
|8nnczx|           5|Rick and Morty Go...| 1527811308|              google|
|8nnd0a|           2|Google listed “Na...| 1527811311|              google|
|8nnd0v|           0|5/31 PBE Update: ...| 1527811316|                 iot|
|8nnd19|           1|Pussy Riot - Make...| 1527811318|                 iot|
|8nnd1g|           1|Spider found in A...| 1527811320|            internet|
|8nnd2e|           0|What is an Ethere...| 1527811328|            ethereum|
|8nnd2q|          13|What are ALL the ...| 1527811330|              monero|
|8nnd6p|          10|Whoever is making...| 1527811362|             bitcoin|
+------+------------+--------------------+-----------+--------------------+
'''


from pyspark.sql.types import StringType

# Define udf
from pyspark.sql.functions import udf
words_b = sc.broadcast(words)
    
#Selecting the POSTS that contains one of the keywords. Adding the keyword as a column.
def findTitlesKeyword(title):
    for word in words_b.value:
        if word in title.lower():
            return word
    return 'a'
    
hadoop = sc._jvm.org.apache.hadoop

fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration() 

path = hadoop.fs.Path("/user/data/submissions/")
for f in fs.get(conf).listStatus(path):
    if((str(f.getPath())[-14:]=='RS_2017-01.bz2') or (str(f.getPath())[-14:]=='RS_2017-02.bz2')):# or (str(f.getPath())[-14:]=='RS_2017-03.bz2')):
        df=spark.read.json(str(f.getPath())).select(['id', 'num_comments', 'title', 'created_utc'])
        udfFindTitlesKeyword=udf(findTitlesKeyword, StringType())
        df_unfiltered = df.withColumn("keyword", udfFindTitlesKeyword("title"))
    
        df_filtered = df_unfiltered.filter(df_unfiltered["keyword"]!="a")
    
        df_filtered.write.option("header", "true").csv("/user/data/submissions_processed/"+str(f.getPath())[-14:])#, mode='append')
    else:
        pass
        
#Loading the data
df_filtered = sqlContext.read.format('csv').option("header", "true").load("/user/data/submissions_processed") 

df_filtered.printSchema()
'''
root
 |-- id: string (nullable = true)
 |-- num_comments: string (nullable = true)
 |-- title: string (nullable = true)
 |-- created_utc: string (nullable = true)
 |-- keyword: string (nullable = true)
 '''

 ## Analyzing Comments

#Changing column names of the POSTS and adding string 't_3' to the id, so that it's in the same format as the comments
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import *

df_filtered = sqlContext.read.format('csv').option("header", "true").load("/user/data/submissions_processed") 
#Adding t3_ in id
adding_string = udf(lambda id_link: "t3_"+id_link, StringType())
df_filtered = df_filtered\
.withColumn('new_ID',adding_string(df_filtered.id))\
.drop(df_filtered.id)\
.select(col('new_ID').alias('id_parent'), col('title'), col('created_utc'), col('keyword'))
df_filtered = df_filtered.withColumnRenamed("created_utc", "created_utc_parent")

#Preprocessing comments. With the given POSTS, we take their comments
hadoop = sc._jvm.org.apache.hadoop

fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration() 

path = hadoop.fs.Path("/user/data/comments/")
for f in fs.get(conf).listStatus(path):
    print(f.getPath())
    df_comments = spark.read.json(str(f.getPath())).select(['author', 'body', 'controversiality', 'created_utc', 'id', 'link_id', 'parent_id', 'score', 'subreddit', 'subreddit_id'])
    df = df_comments.join(df_filtered, df_comments.link_id == df_filtered.id_parent)
    df.write.option("header", "true").csv("/user/data/comments_complete/"+str(f.getPath())[-14:-4])

'''
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2017-01.bz2
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2017-02.bz2
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2017-03.bz2
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2017-04.bz2
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2017-05.bz2
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2017-06.bz2
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2017-07.bz2
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2017-08.bz2
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2017-09.bz2
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2017-10.bz2
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2017-11.bz2
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2017-12.bz2
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2018-01.bz2
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2018-02.bz2
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2018-03.bz2
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2018-04.bz2
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2018-05.bz2
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2018-06.bz2
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2018-07.bz2
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments/RC_2018-08.bz2
'''

%spark2.pyspark
#Saving the dataset with the merged information of the comments and POSTS in a one folder .csv
hadoop = sc._jvm.org.apache.hadoop

fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration() 

path = hadoop.fs.Path("/user/data/comments_complete/")
for f in fs.get(conf).listStatus(path):
    print(f.getPath())
    df = sqlContext.read.format('csv').option("header", "true").load(str(f.getPath())) 
    df.select(['author', 'body', 'controversiality', 'created_utc', 'id', 'link_id', 'parent_id', 'score', 'subreddit', 'subreddit_id', 'title', 'created_utc_parent', 'keyword']).repartition(1).write.option("header", "true").option('quote', '"').option('escape', '"').csv("/user/data/comments_csv_escape/"+str(f.getPath())[-10:])

'''
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2017-01
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2017-02
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2017-03
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2017-04
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2017-05
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2017-06
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2017-07
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2017-08
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2017-09
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2017-10
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2017-11
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2017-12
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2018-01
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2018-02
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2018-03
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2018-04
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2018-05
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2018-06
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2018-07
hdfs://op-engineroom-hd-1.eurecat.local.org:8020/user/data/comments_complete/RC_2018-08
'''



