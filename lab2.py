from pyspark.sql.functions import window, count, sum as _sum, avg, round as _round, col, desc, asc, to_timestamp, lit

gdansk_min_avg = (
    df.filter(col("store") == "Gdańsk")
    .groupBy(window("timestamp", "1 hour"))
    .agg(
        _round(avg("amount"), 2).alias("srednia_PLN"),
        count("tx_id").alias("liczba_tx"),
    )
    .select(
        col("window.start").alias("od"),
        col("window.end").alias("do"),
        "liczba_tx",
        "srednia_PLN",
    )
    .orderBy(asc("srednia_PLN"))
)
gdansk_min_avg.limit(1).show(truncate=False)

window_start = "2024-01-01 09:00:00"
window_end   = "2024-01-01 09:30:00"

kategorie_okno = (
    df.filter(
        (col("timestamp") >= to_timestamp(lit(window_start), "yyyy-MM-dd HH:mm:ss")) &
        (col("timestamp") <  to_timestamp(lit(window_end),   "yyyy-MM-dd HH:mm:ss"))
    )
    .groupBy("category")
    .agg(
        count("tx_id").alias("liczba_tx"),
        _round(_sum("amount"), 2).alias("suma_PLN"),
    )
    .orderBy(desc("liczba_tx"))
)
kategorie_okno.show(truncate=False)

szczyt_15min = (
    df.groupBy(window("timestamp", "15 minutes"))
    .agg(
        count("tx_id").alias("liczba_tx"),
        _round(_sum("amount"), 2).alias("suma_PLN"),
    )
    .select(
        col("window.start").alias("od"),
        col("window.end").alias("do"),
        "liczba_tx",
        "suma_PLN",
    )
    .orderBy(desc("liczba_tx"))
)
szczyt_15min.limit(1).show(truncate=False)
