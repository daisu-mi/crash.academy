flights<-sql("select * from airline_ontime.flights")
is_late<-ifelse(flights$arrdelay < 0, "ontime", "late")
is_am<-ifelse(flights$crsdeptime < 1200, "yes", "no")
is_winter<-ifelse(flights$month > 11 | flights$month < 3, "yes", "no")
is_top10origin<-ifelse(flights$origin == "ATL" | flights$origin =="LAX" | flights$origin == "ORD" | flights$origin == "BOS" | flights$origin == "MCO" | flights$origin == "CLT" | flights$origin == "CLT" | flights$origin == "SFO" | flights$origin == "FLL" | flights$origin == "MIA" | flights$origin == "DCA", "yes", "no")

flights<-withColumn(flights, "is_late", is_late)
flights<-withColumn(flights, "is_am", is_am)
flights<-withColumn(flights, "is_winter", is_winter)
flights<-withColumn(flights, "is_top10origin", is_top10origin)

flights_model <- spark.kmeans(flights, is_late ~ is_am + is_winter + is_top10origin, k = 2)
flights_pred <- predict(flights_model, flights)

showDF(select(flights_pred, "is_late", "prediction"))

crosstab(flights_pred, "is_late", "prediction")

