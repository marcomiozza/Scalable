# Earthquake Co-Occurrence – Esecuzione su Dataproc (Dataset di Test)

## 1. Autenticazione

```bash
gcloud auth login
gcloud config set project scalable-project
```

---

## 2. Creazione bucket (region us-central1)

```bash
gsutil mb -l us-central1 gs://earthquake-bucket
```

---

## 3. Build del progetto

Dalla root della repository:

```bash
sbt clean package
```

Il JAR generato è:

```
target/scala-2.12/earthquake-spark_2.12-0.1.0-SNAPSHOT.jar
```

---

## 4. Upload JAR e dataset di test

Upload del JAR:

```bash
gsutil cp target/scala-2.12/earthquake-spark_2.12-0.1.0-SNAPSHOT.jar \
gs://earthquake-bucket/jars/earthquake.jar
```

Il dataset di test è nella repository:

```
data/test.csv
```

Upload del dataset:

```bash
gsutil cp data/test.csv \
gs://earthquake-bucket/data/test.csv
```

---

## 5. Creazione cluster Dataproc (2 worker)

```bash
gcloud dataproc clusters create eq-w2 \
  --region=us-central1 \
  --num-workers=2 \
  --master-machine-type=n2-standard-4 \
  --worker-machine-type=n2-standard-4 \
  --master-boot-disk-size=240 \
  --worker-boot-disk-size=240
```

---

## 6. Sottomissione del job Spark

```bash
gcloud dataproc jobs submit spark \
  --cluster=eq-w2 \
  --region=us-central1 \
  --jar=gs://earthquake-bucket/jars/earthquake.jar \
  -- \
  gs://earthquake-bucket/data/test.csv \
  gs://earthquake-bucket/output/test \
  24
```

Parametri dell’applicazione:

1. Percorso input CSV  
2. Percorso output  
3. Numero di partizioni (24 per 2 worker)

---

## 7. Eliminazione cluster

```bash
gcloud dataproc clusters delete eq-w2 --region=us-central1
```
