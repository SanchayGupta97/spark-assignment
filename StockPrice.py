from pyspark.sql import SparkSession
if __name__ == "__main__":
    session = SparkSession.builder.appName("StockPrices").master("local[*]").getOrCreate()

    StockPriceList = session.read.option("header", "true") \
        .option("inferSchema", value=True).csv("stock_prices.csv")

    # StockPriceList.show()
    StockPriceList.createOrReplaceTempView("StockPriceList")
    # average_price_list = session.sql("SELECT date, AVG((close-open)*volume) as Average_Price from StockPriceList GROUP BY date")
    # average_price_list.coalesce(1).write.format("csv").option("header", "true")\
    #     .save("averageprice.csv")

    most_traded_stock = session.sql("select ticker, avgprice from(select ticker, avg(close*volume) as avgprice from StockPriceList group by ticker)where avgprice=(select  max(avgprice)from (select ticker, avg(close*volume) as avgprice from StockPricelist group by ticker))")
    most_traded_stock.coalesce(1).write.format("csv").option("header", "true").save("most.csv")

    