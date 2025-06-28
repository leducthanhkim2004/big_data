import pyspark.pandas as ps
# HDFS path to your file

df = ps.read_csv("hdfs://192.168.1.61:9000/shared/IMDB-Dataset.csv")
print(df.head())
print(df.info())
print(df.groupby('sentiment').count())

# Filtering
positive = df[df['sentiment'] == 'positive']
print(positive.head())


