from pyspark.sql import SparkSession , types, functions,udf
from pyspark.sql.functions import Column,udf


spark=SparkSession.builder.master("local").appName("IPL").getOrCreate()

df=spark.read.csv("matches.csv")
total_df=df.select(df[0].alias("ID"),df[1].alias("Season"),df[2].alias("City"),df[3].alias("Date"),df[4].alias("Team1"),df[5].alias("Team2"),df[6].alias("toss_winner"),df[7].alias("toss_decision"),df[8].alias("result"),df[9].alias("dl_applied"),df[10].alias("winner"),df[11].alias("win_by_runs"),df[12].alias("win_by_wickets"),df[13].alias("player_of_match"),df[14].alias("venue"),df[15].alias("umpire1"),df[16].alias("umpire2"),df[17].alias("umpire3"))
stadium_analysis_df=total_df.select("toss_winner", "win_by_runs","win_by_wickets","venue")
print(stadium_analysis_df.show())
print(type(stadium_analysis_df))

#find number of first bat win  for each unique venue
venue_firstBat=stadium_analysis_df.rdd.map(lambda x:(x[3],int(x[1]))).filter(lambda x:x[1] >0)
venue_firstBat1=venue_firstBat.map(lambda x:(x[0],1)).reduceByKey(lambda a,b:a+b).sortBy(lambda x:x[1],ascending=False)
print(venue_firstBat1.collect())

#find number of first bowl win for each unique venue
venue_firstBowl=stadium_analysis_df.rdd.map(lambda x:(x[3],int(x[2]))).filter(lambda x:x[1] >0)
venue_firstBowl1=venue_firstBowl.map(lambda x:(x[0],1)).reduceByKey(lambda a,b:a+b).sortBy(lambda x:x[1],ascending=False)
print(venue_firstBowl.collect())

#join bat first and bowl first rdds
best_venue_analysis=venue_firstBat1.join(venue_firstBowl1)
# print(best_venue_analysis.collect())
# print(type(best_venue_analysis))

#calculate the percentage of first bat win by total matches with results
# no.of.bat.first.wins divide by total

best_venue_analysis1=best_venue_analysis.map(lambda x:(x[0],(x[1][0])/(x[1][0]+x[1][1])*100))
print("Below are the best venue to BAT first")
print(best_venue_analysis1.collect())
