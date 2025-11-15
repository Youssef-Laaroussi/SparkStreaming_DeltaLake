from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col, window, avg, stddev, lead, isnan, when, lit, count, sum as _sum, expr, current_timestamp
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from delta.tables import *
import os
import json
from datetime import datetime

# === Spark session optimisée avec Delta Lake ===
spark = SparkSession.builder \
    .appName("RealTimeStockPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
    .config("spark.sql.streaming.minBatchesToRetain", "2") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .getOrCreate()

# Réduire le bruit des logs
spark.sparkContext.setLogLevel("WARN")

# === Schéma JSON ===
schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("volume", IntegerType()) \
    .add("timestamp", TimestampType())

# === Lecture Kafka streaming optimisée ===
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_prices") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", "1000") \
    .option("failOnDataLoss", "false") \
    .load()

df_parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# === Bronze Delta (raw data) ===
bronze_query = df_parsed.writeStream.format("delta") \
    .option("checkpointLocation", "data/bronze/_checkpoint") \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .start("data/bronze")

# === Silver Delta (deduplicated + data quality) ===
df_silver = df_parsed.dropDuplicates(["symbol","timestamp"]) \
    .filter(col("price") > 0) \
    .filter(col("volume") > 0) \
    .filter(col("symbol").isNotNull())

silver_query = df_silver.writeStream.format("delta") \
    .option("checkpointLocation", "data/silver/_checkpoint") \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .start("data/silver")

# === Gold Delta (aggregations STABLES) ===
agg_df = df_silver.withWatermark("timestamp", "30 seconds") \
    .groupBy(window(col("timestamp"), "10 seconds"), col("symbol")) \
    .agg(
        avg("price").alias("avg_price"),
        stddev("price").alias("volatility"),
        count("price").alias("trade_count")
    )

gold_query = agg_df.writeStream.format("delta") \
    .option("checkpointLocation", "data/gold/_checkpoint") \
    .option("mergeSchema", "true") \
    .outputMode("complete") \
    .start("data/gold")

# === ANALYSE SPARK SQL AVEC/SANS CACHE ===
def analyze_with_sql():
    """Analyse comparative avec/sans cache"""
    print("\n" + "="*50)
    print("ANALYSE SPARK SQL - AVEC/SANS CACHE")
    print("="*50)
    
    try:
        # Lecture des données Gold
        df_gold = spark.read.format("delta").load("data/gold")
        
        if df_gold.count() == 0:
            print("⚠️  Aucune donnée dans la table Gold")
            return
        
        df_gold.createOrReplaceTempView("stock_aggregations")
        
        complex_query = """
        SELECT 
            symbol,
            AVG(avg_price) as mean_price,
            AVG(volatility) as mean_volatility,
            COUNT(*) as aggregation_count
        FROM stock_aggregations 
        WHERE volatility IS NOT NULL 
        GROUP BY symbol
        ORDER BY mean_volatility DESC
        """
        
        print("\n1. EXÉCUTION SANS CACHE:")
        df_no_cache = spark.sql(complex_query)
        print("Résultats sans cache:")
        df_no_cache.show(truncate=False)
        
        print("\n2. EXÉCUTION AVEC CACHE:")
        df_cached = spark.sql(complex_query).cache()
        df_cached.count()
        print("Résultats avec cache:")
        df_cached.show(truncate=False)
        
        import time
        start_time = time.time()
        result_no_cache = spark.sql(complex_query).collect()
        time_no_cache = time.time() - start_time
        
        start_time = time.time()
        result_cached = df_cached.collect()
        time_cached = time.time() - start_time
        
        print(f"\n📊 COMPARAISON PERFORMANCE:")
        print(f"Temps sans cache: {time_no_cache:.4f}s")
        print(f"Temps avec cache: {time_cached:.4f}s")
        if time_no_cache > 0:
            print(f"✅ Amélioration: {((time_no_cache - time_cached) / time_no_cache * 100):.1f}%")
        
        df_cached.unpersist()
        
    except Exception as e:
        print(f"❌ Erreur analyse SQL: {e}")

# === ML STREAMING ===
MODEL_PATH = "data/models/rf_model"
MIN_ROWS = 5

def train_predict(batch_df, batch_id):
    print(f"\n=== Batch {batch_id} ===")
    print(f"Lignes reçues: {batch_df.count()}")

    if batch_df.count() == 0:
        return

    try:
        batch_df_cached = batch_df.cache()
        batch_df_cached.count()

        # Agrégations batch
        df_summary = batch_df_cached.groupBy("symbol").agg(
            avg("price").alias("avg_price"),
            stddev("price").alias("volatility"),
            count("price").alias("message_count")
        )
        print("\n--- Agrégations batch ---")
        df_summary.show(truncate=False)

        # Création labels
        window_spec = Window.partitionBy("symbol").orderBy("timestamp")
        df_labeled = batch_df_cached.withColumn("future_price", lead("price", 1).over(window_spec))
        df_labeled = df_labeled.filter(col("future_price").isNotNull())
        df_labeled = df_labeled.withColumn("label", when(col("future_price") > col("price"), 1).otherwise(0))

        print(f"Lignes après création labels: {df_labeled.count()}")

        if df_labeled.count() < MIN_ROWS:
            print(f"Batch {batch_id} insuffisant pour training")
            if os.path.exists(MODEL_PATH):
                try:
                    model = RandomForestClassificationModel.load(MODEL_PATH)
                    assembler = VectorAssembler(inputCols=["price", "volume"], outputCol="features")
                    df_features = assembler.transform(df_labeled)
                    predictions = model.transform(df_features)
                    
                    correct = predictions.filter(col("prediction") == col("label")).count()
                    total = predictions.count()
                    accuracy = correct / total if total > 0 else 0
                    
                    print(f"📊 Prédictions modèle existant - Accuracy: {accuracy:.3f}")
                    predictions.select("symbol", "price", "label", "prediction").show(truncate=False)
                    
                except Exception as e:
                    print(f"Erreur chargement modèle: {e}")
            return

        # Entraînement nouveau modèle
        assembler = VectorAssembler(inputCols=["price", "volume"], outputCol="features")
        rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=10, seed=42)
        pipeline = Pipeline(stages=[assembler, rf])
        
        df_train, df_test = df_labeled.randomSplit([0.8, 0.2], seed=42)
        
        if df_train.count() == 0 or df_test.count() == 0:
            return
        
        model = pipeline.fit(df_train)
        predictions = model.transform(df_test)
        
        correct = predictions.filter(col("prediction") == col("label")).count()
        total = predictions.count()
        accuracy = correct / total if total > 0 else 0
        
        # Sauvegarde modèle
        if not os.path.exists("data/models"):
            os.makedirs("data/models")
        model.write().overwrite().save(MODEL_PATH)
        
        print(f"\n🎯 MODÈLE BATCH {batch_id}")
        print(f"✅ Accuracy: {accuracy:.3f}")
        print(f"✅ Modèle sauvegardé")
        
        print("\n--- Prédictions ---")
        predictions.select("symbol", "price", "label", "prediction").show(truncate=False)
        
    except Exception as e:
        print(f"Erreur batch {batch_id}: {e}")
    finally:
        try:
            batch_df_cached.unpersist()
        except:
            pass

# === LANCEMENT ===
if __name__ == "__main__":
    print("🚀 PIPELINE SPARK STREAMING - DÉMARRAGE")
    print("📊 Architecture: Bronze → Silver → Gold")
    print("🤖 ML Temps Réel")
    print("💾 Cache/No-Cache Analysis")
    
    # Analyse SQL
    analyze_with_sql()
    
    # Streaming ML
    print("\n🎯 DÉMARRAGE STREAMING ML...")
    query_ml = df_silver.writeStream \
        .foreachBatch(train_predict) \
        .option("checkpointLocation", "data/ml_checkpoint") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    print("✅ Pipeline démarré - En attente de données Kafka...")
    
    # Attente avec meilleure gestion d'erreurs
    try:
        query_ml.awaitTermination()
    except Exception as e:
        print(f"🛑 Arrêt pipeline: {e}")
        # Arrêt propre des autres requêtes
        try:
            bronze_query.stop()
            silver_query.stop()
            gold_query.stop()
        except:
            pass
        spark.stop()
