# 🚀 Complete Data Platform

A production-ready data platform with Apache Spark, Apache Airflow, Jupyter, MinIO (S3), Dremio, and Nessie.

## 🎯 What's Included

- **Apache Spark** - Distributed data processing (Master + Worker + History Server)
- **Apache Airflow** - Workflow orchestration and scheduling
- **Jupyter** - Interactive notebooks with Spark integration
- **MinIO** - S3-compatible object storage
- **PostgreSQL** - Metadata database for Airflow
- **Dremio** - Data lakehouse query engine
- **Nessie** - Data catalog for Git-like version control

## 🚀 Quick Start

### Step 1: Download Required JAR Files

The `dockerfile/` folder requires three large dependency JARs (> 100 MB each). Download them manually and place them in the `dockerfile/` directory:

| File | Size | Download Link |
|------|------|---------------|
| `hudi-spark3.4-bundle_2.12-0.14.0.jar` | ≈ 100 MB | [Maven Central](https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark3.4-bundle) |
| `aws-java-sdk-bundle-1.12.262.jar` | ≈ 268 MB | [Maven Central](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle/1.12.262) • [Direct Download](https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/) |
| `hadoop-aws-3.3.4.jar` | ≈ 75 MB | [Maven Central](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/3.3.4) • [Direct Download](https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/) |

### Step 2: Run the Setup Script

```bash
chmod +x complete.sh
./complete.sh
```

### Step 3: Start the Platform

```bash
docker compose -p data_platform up -d
```

### Step 4: Verify Services

```bash
docker compose -p data_platform ps
```

All services should show as "Up" or "healthy".

## 🌐 Access URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | http://localhost:8085 | Username: `admin`<br>Password: `admin` |
| **Spark Master** | http://localhost:9090 | No auth required |
| **Spark Worker** | http://localhost:8081 | No auth required |
| **Spark History** | http://localhost:18080 | No auth required |
| **Jupyter** | http://localhost:8888 | See token below* |
| **MinIO Console** | http://localhost:9001 | Username: `admin`<br>Password: `password` |
| **MinIO API** | http://localhost:9000 | - |
| **Dremio** | http://localhost:9047 | Setup on first visit |
| **Nessie** | http://localhost:19120 | No auth required |
| **PostgreSQL** | localhost:5432 | Username: `airflow`<br>Password: `airflow`<br>Database: `airflow` |

### Getting Jupyter Token

```bash
docker compose -p data_platform logs jupyter-spark | grep token
```

Look for a line like: `http://127.0.0.1:8888/?token=abc123...`

## 📚 Example Usage

### 1. Run a Spark Job from Airflow

The platform includes an example DAG (`example_spark_dag`) that you can trigger from the Airflow UI:

1. Open http://localhost:8085
2. Login with `admin` / `admin`
3. Find `example_spark_dag` in the DAG list
4. Toggle it to "On" and click "Trigger DAG"

### 2. Use Jupyter Notebooks

1. Get the Jupyter token (see above)
2. Open http://localhost:8888
3. Open `01_getting_started.ipynb`
4. Run the cells to test Spark integration

### 3. Submit a Spark Job Manually

```bash
docker exec -it spark-master bash

/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/example_spark_job.py
```

### 4. Access MinIO Storage

1. Open http://localhost:9001
2. Login with `admin` / `password`
3. Browse the `warehouse` and `test` buckets
4. Upload/download files as needed

## 🛠️ Common Commands

### View Logs

```bash
# All services
docker compose -p data_platform logs -f

# Specific service
docker compose -p data_platform logs -f airflow-webserver
docker compose -p data_platform logs -f spark-master
docker compose -p data_platform logs -f jupyter-spark
```

### Restart Services

```bash
# Restart all
docker compose -p data_platform restart

# Restart specific service
docker compose -p data_platform restart spark-worker
```

### Stop/Start Services

```bash
# Stop all services
docker compose -p data_platform down

# Start all services
docker compose -p data_platform up -d

# Stop and remove all data (⚠️ CAUTION!)
docker compose -p data_platform down -v
```

### Enter Container Shell

```bash
docker exec -it spark-master bash
docker exec -it airflow-webserver bash
docker exec -it jupyter-spark bash
```

### Check Service Status

```bash
docker compose -p data_platform ps
```

## 📁 Directory Structure

```
data-platform/
├── docker-compose.yml          # Main orchestration file
├── airflow.env                 # Airflow configuration
├── complete.sh                 # Setup script
├── README.md                   # This file
├── .gitignore                  # Git ignore rules
│
├── dockerfile/                 # Docker build files
│   ├── hudi-spark3.4-bundle_2.12-0.14.0.jar
│   ├── aws-java-sdk-bundle-1.12.262.jar
│   └── hadoop-aws-3.3.4.jar
│
├── dags/                       # Airflow DAGs
│   └── example_dag.py
│
├── jobs/                       # Spark job scripts
│   └── example_spark_job.py
│
├── notebooks/                  # Jupyter notebooks
│   └── 01_getting_started.ipynb
│
├── spark-conf/                 # Spark configuration
│   └── spark-defaults.conf
│
└── [data directories]/         # Created automatically
    ├── logs/
    ├── spark-events/
    ├── spark-logs/
    ├── spark-work/
    ├── postgres/
    ├── minio/
    ├── nessie/
    └── dremio/
```

## 🔧 Customization

### Adjust Spark Resources

Edit `docker-compose.yml` and modify:

```yaml
spark-worker:
  environment:
    SPARK_WORKER_CORES: "4"      # Change number of cores
    SPARK_WORKER_MEMORY: 4g      # Change memory allocation
```

### Add New DAGs

1. Create a Python file in the `dags/` directory
2. Airflow will automatically detect it within 30 seconds
3. Refresh the Airflow UI to see your new DAG

### Add New Spark Jobs

1. Create a Python file in the `jobs/` directory
2. Submit it using spark-submit or from Airflow

### Scale Spark Workers

To add more Spark workers, duplicate the `spark-worker` service in `docker-compose.yml` and change the service name and container name.

## 🐛 Troubleshooting

### Services Won't Start

```bash
# Check logs for specific service
docker compose -p data_platform logs [service-name]

# Common fix: remove and recreate
docker compose -p data_platform down
docker compose -p data_platform up -d
```

### Permission Errors

```bash
# Fix permissions on data directories
chmod -R 777 dags jobs logs notebooks spark-events spark-logs spark-work
```

### Port Already in Use

```bash
# Find what's using the port
sudo lsof -i :8085  # or any other port

# Either stop that service or change port in docker-compose.yml
```

### Out of Memory

Reduce resource allocation in `docker-compose.yml`:
- Lower `SPARK_WORKER_MEMORY`
- Reduce `SPARK_WORKER_CORES`
- Limit number of parallel tasks in Airflow

### Cannot Connect to Spark Master

```bash
# Check if Spark Master is running
docker compose -p data_platform ps spark-master

# View logs
docker compose -p data_platform logs spark-master

# Restart Spark services
docker compose -p data_platform restart spark-master spark-worker
```

### Jupyter Can't Connect to Spark

```bash
# Ensure Spark Master is accessible
docker exec -it jupyter-spark ping spark-master

# Check Jupyter logs
docker compose -p data_platform logs jupyter-spark

# Restart Jupyter
docker compose -p data_platform restart jupyter-spark
```

### Missing JAR Files Error

If you see errors about missing JAR files during Docker build:
1. Verify all three JAR files are in the `dockerfile/` directory
2. Check file names match exactly (case-sensitive)
3. Verify file sizes match the expected sizes

## 📊 Performance Tips

1. **Increase Spark Memory**: Edit `SPARK_WORKER_MEMORY` for larger datasets
2. **Add More Workers**: Scale the spark-worker service in docker-compose.yml
3. **Enable Spark UI**: Access http://localhost:4040 when a job is running
4. **Monitor Resources**: Use `docker stats` to monitor container resources
5. **Clean Old Logs**: The platform auto-cleans logs older than 7 days
6. **Partition Your Data**: Use appropriate partitioning strategies in Spark jobs
7. **Tune Spark Configs**: Adjust `spark-defaults.conf` for your workload

## 🔒 Security Notes

**⚠️ This is a development setup - DO NOT use in production without:**

1. Changing all default passwords
2. Enabling SSL/TLS for all services
3. Configuring proper authentication and authorization
4. Setting up network security and firewalls
5. Implementing access controls and RBAC
6. Using secrets management (e.g., Vault, AWS Secrets Manager)
7. Regular security updates and patching
8. Proper backup and disaster recovery procedures

## 📖 Additional Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Jupyter Documentation](https://jupyter.org/documentation)
- [MinIO Documentation](https://min.io/docs/)
- [Dremio Documentation](https://docs.dremio.com/)
- [Nessie Documentation](https://projectnessie.org/docs/)

## 🆘 Getting Help

If you encounter issues:

1. Check the logs: `docker compose -p data_platform logs -f [service-name]`
2. Verify all services are running: `docker compose -p data_platform ps`
3. Ensure you have enough resources (RAM/disk space)
4. Check firewall/antivirus isn't blocking ports
5. Verify JAR files are downloaded correctly

## 📝 Version Info

- Spark: 3.4
- Airflow: 2.7
- Python: 3.9
- PostgreSQL: Latest
- MinIO: Latest
- Dremio: Latest
- Nessie: Latest

## 🎓 Learning Path

1. **Start with Jupyter** - Run `01_getting_started.ipynb` to understand Spark basics
2. **Create an Airflow DAG** - Modify `example_dag.py` to schedule your own workflows
3. **Write a Spark Job** - Create a new job in `jobs/` directory
4. **Explore MinIO** - Upload data and query it with Spark
5. **Try Dremio** - Create a data source and run SQL queries
6. **Use Nessie** - Explore data versioning and catalog features

## 🔐 Default Credentials Summary

| Service | Username | Password | Database (if applicable) |
|---------|----------|----------|--------------------------|
| Airflow | `admin` | `admin` | - |
| MinIO | `admin` | `password` | - |
| PostgreSQL | `airflow` | `airflow` | `airflow` |

## 🧹 Cleanup

To completely remove the platform and all data:

```bash
# Stop and remove containers, networks, and volumes
docker compose -p data_platform down -v

# Remove data directories (optional)
rm -rf logs spark-events spark-logs spark-work postgres minio nessie dremio
```

---

**Happy Data Engineering! 🚀**

For questions or issues, contact: `en.sonukumar@gmail.com`

---

