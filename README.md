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

## Quick start

Step 1: Run the setup script  
```bash
chmod +x complete.sh
./complete.sh

## Large dependency JARs for dockerfile

The `dockerfile/` folder contains three big binaries (> 100 MB).  
They are **not** included in the repo directly—only Git-LFS pointer files are committed.  
please download them manually use the links below and store in same folder/dir.

| File | Size | Maven Central |
|------|------|---------------|
| `hudi-spark3.4-bundle_2.12-0.14.0.jar` | ≈ 100 MB | [mvnrepo](https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark3.4-bundle) |
| `aws-java-sdk-bundle-1.12.262.jar` | ≈ 268 MB | [mvnrepo](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle/1.12.262) • [direct](https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/) |
| `hadoop-aws-3.3.4.jar` | ≈ 75 MB | [mvnrepo](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/3.3.4) • [direct](https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/) |

After cloning, restore the real files with:

```bash
git lfs pull

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
docker-compose logs jupyter-spark | grep token
```

Look for a line like: `http://127.0.0.1:8888/?token=abc123...`

## 📚 Example Usage

### 1. Run a Spark Job from Airflow

The platform includes an example DAG (`example_spark_dag`) that you can trigger from the Airflow UI.

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
2. Login with admin/password
3. Browse the `warehouse` and `test` buckets
4. Upload/download files as needed

## 🛠️ Common Commands

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-webserver
docker-compose logs -f spark-master
docker-compose logs -f jupyter-spark
```

### Restart Services
```bash
# Restart all
docker-compose restart

# Restart specific service
docker-compose restart spark-worker
```

### Stop/Start Services
```bash
# Stop all services
docker-compose down

# Start all services
docker-compose up -d

# Stop and remove all data (CAUTION!)
docker-compose down -v
```

### Enter Container Shell
```bash
docker exec -it spark-master bash
docker exec -it airflow-webserver bash
docker exec -it jupyter-spark bash
```

### Check Service Status
```bash
docker-compose ps
```

## 📁 Directory Structure

```
data-platform/
├── docker-compose.yml          # Main orchestration file
├── airflow.env                 # Airflow configuration
├── README.md                   # This file
├── .gitignore                 # Git ignore rules
│
├── dags/                      # Airflow DAGs
│   └── example_dag.py
│
├── jobs/                      # Spark job scripts
│   └── example_spark_job.py
│
├── notebooks/                 # Jupyter notebooks
│   └── 01_getting_started.ipynb
│
├── spark-conf/               # Spark configuration
│   └── spark-defaults.conf
│
└── [data directories]/       # Created automatically
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

## 🐛 Troubleshooting

### Services Won't Start

```bash
# Check logs for specific service
docker-compose logs [service-name]

# Common fix: remove and recreate
docker-compose down
docker-compose up -d
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

### Cannot Connect to Spark Master

```bash
# Check if Spark Master is running
docker-compose ps spark-master

# View logs
docker-compose logs spark-master

# Restart Spark services
docker-compose restart spark-master spark-worker
```

### Jupyter Can't Connect to Spark

```bash
# Ensure Spark Master is accessible
docker exec -it jupyter-spark ping spark-master

# Check Jupyter logs
docker-compose logs jupyter-spark
```

## 📊 Performance Tips

1. **Increase Spark Memory**: Edit `SPARK_WORKER_MEMORY` for larger datasets
2. **Add More Workers**: Duplicate the spark-worker service in docker-compose.yml
3. **Enable Spark UI**: Access http://localhost:4040 when a job is running
4. **Monitor Resources**: Use `docker stats` to monitor container resources
5. **Clean Old Logs**: The platform auto-cleans logs older than 7 days

## 🔒 Security Notes

**⚠️ This is a development setup - DO NOT use in production without:**

1. Changing default passwords
2. Enabling SSL/TLS
3. Configuring authentication
4. Setting up network security
5. Implementing access controls

## 📖 Additional Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Jupyter Documentation](https://jupyter.org/documentation)
- [MinIO Documentation](https://min.io/docs/)
- [Dremio Documentation](https://docs.dremio.com/)

## 🆘 Getting Help

If you encounter issues:

1. Check the logs: `docker-compose logs -f [service-name]`
2. Verify all services are running: `docker-compose ps`
3. Ensure you have enough resources (RAM/disk)
4. Check firewall/antivirus isn't blocking ports

## 📝 Version Info

- Spark: 3.4
- Airflow: 2.7
- Python: 3.9
- PostgreSQL: Latest
- MinIO: Latest

## 🎓 Learning Path

1. **Start with Jupyter** - Run `01_getting_started.ipynb`
2. **Create an Airflow DAG** - Modify `example_dag.py`
3. **Write a Spark Job** - Create a new job in `jobs/`
4. **Explore MinIO** - Upload data and query it with Spark
5. **Try Dremio** - Create a data source and run SQL queries

---

## 🔐 Default Credentials

| Service | Username | Password |
|----------|-----------|-----------|
| Airflow | `admin` | `admin` |
| MinIO | `admin` | `password` |
| Postgres | `airflow` | `airflow` |

---

## ✅ Run the Project

```bash
docker compose -p data_platform up -d
```

Stop the entire project:

```bash
docker compose -p data_platform down
```


**Happy Data Engineering! 🚀**

For questions or issues: `en.sonukumar@gmail.com`
