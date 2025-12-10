#!/bin/bash

# Complete Package Creator for Data Platform
# This script creates everything and packages it for sharing

set -e

echo "=================================================="
echo "  📦 Data Platform Package Creator"
echo "=================================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# Check if docker-compose.yml exists
if [ ! -f "docker-compose.yml" ]; then
    echo -e "${RED}❌ Error: docker-compose.yml not found in current directory${NC}"
    echo "Please make sure docker-compose.yml is in the same directory as this script"
    exit 1
fi

echo -e "${BLUE}Step 1/5: Creating directory structure...${NC}"
mkdir -p dags jobs logs notebooks spark-events minio postgres nessie dremio
mkdir -p spark-logs/master spark-logs/worker spark-logs/history spark-logs/jupyter
mkdir -p spark-work spark-conf
echo -e "${GREEN}✓ Directories created${NC}"

echo -e "${BLUE}Step 2/5: Creating configuration files...${NC}"

# Create airflow.env
cat > airflow.env << 'EOF'
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
AIRFLOW__CORE__FERNET_KEY=UKMzEm3yIuFYEq1y3-2FxPNWSVwRASpahmQ9kQfEr8E=
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
AIRFLOW__LOGGING__BASE_LOG_FOLDER=/opt/airflow/logs
AIRFLOW__CORE__PARALLELISM=32
AIRFLOW__CORE__DAG_CONCURRENCY=16
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=16
EOF

# Create spark-defaults.conf
cat > spark-conf/spark-defaults.conf << 'EOF'
spark.master                     spark://spark-master:7077
spark.eventLog.enabled           true
spark.eventLog.dir               /opt/spark-events
spark.history.fs.logDirectory    /opt/spark-events
spark.sql.warehouse.dir          s3a://warehouse/
spark.hadoop.fs.s3a.endpoint     http://minio:9000
spark.hadoop.fs.s3a.access.key   admin
spark.hadoop.fs.s3a.secret.key   password
spark.hadoop.fs.s3a.path.style.access    true
spark.hadoop.fs.s3a.impl         org.apache.hadoop.fs.s3a.S3AFileSystem
spark.sql.extensions             org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.spark_catalog  org.apache.iceberg.spark.SparkSessionCatalog
spark.sql.catalog.spark_catalog.type     hive
spark.sql.catalog.nessie         org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.nessie.uri     http://nessie:19120/api/v1
spark.sql.catalog.nessie.ref     main
spark.sql.catalog.nessie.warehouse       s3a://warehouse/
spark.sql.catalog.nessie.catalog-impl    org.apache.iceberg.nessie.NessieCatalog
EOF

echo -e "${GREEN}✓ Configuration files created${NC}"

echo -e "${BLUE}Step 3/5: Creating example files...${NC}"

# Create example DAG
cat > dags/example_dag.py << 'EOF'
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'example_spark_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    task1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    task2 = BashOperator(
        task_id='check_spark',
        bash_command='echo "Spark Master: spark://spark-master:7077"',
    )

    task1 >> task2
EOF

# Create example PySpark job
cat > jobs/example_spark_job.py << 'EOF'
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("ExampleJob") \
        .getOrCreate()
    
    # Create sample data
    data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]
    df = spark.createDataFrame(data, ["id", "name", "age"])
    
    print("Sample DataFrame:")
    df.show()
    
    # Perform some operations
    avg_age = df.groupBy().avg("age").collect()[0][0]
    print(f"Average age: {avg_age}")
    
    spark.stop()

if __name__ == "__main__":
    main()
EOF

# Create example Jupyter notebook
cat > notebooks/01_getting_started.ipynb << 'EOF'
{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Getting Started with Spark\n",
    "This notebook demonstrates basic Spark operations in the data platform"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql.functions import col, count, avg\n",
    "\n",
    "# Create Spark session\n",
    "spark = SparkSession.builder \\\n",
    "    .appName(\"GettingStarted\") \\\n",
    "    .master(\"spark://spark-master:7077\") \\\n",
    "    .getOrCreate()\n",
    "\n",
    "print(f\"✅ Spark {spark.version} session created successfully!\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Create a sample DataFrame\n",
    "data = [\n",
    "    (1, \"Alice\", 25, \"Engineering\"),\n",
    "    (2, \"Bob\", 30, \"Sales\"),\n",
    "    (3, \"Charlie\", 35, \"Engineering\"),\n",
    "    (4, \"Diana\", 28, \"Marketing\"),\n",
    "    (5, \"Eve\", 32, \"Engineering\")\n",
    "]\n",
    "\n",
    "columns = [\"id\", \"name\", \"age\", \"department\"]\n",
    "df = spark.createDataFrame(data, columns)\n",
    "\n",
    "print(\"📊 Sample DataFrame:\")\n",
    "df.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Basic operations\n",
    "print(\"📈 Department Statistics:\")\n",
    "dept_stats = df.groupBy(\"department\") \\\n",
    "    .agg(\n",
    "        count(\"*\").alias(\"count\"),\n",
    "        avg(\"age\").alias(\"avg_age\")\n",
    "    ) \\\n",
    "    .orderBy(\"count\", ascending=False)\n",
    "\n",
    "dept_stats.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Filter example\n",
    "print(\"🔍 Engineering employees:\")\n",
    "engineering = df.filter(col(\"department\") == \"Engineering\")\n",
    "engineering.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Write to MinIO (S3)\n",
    "output_path = \"s3a://test/sample_data\"\n",
    "print(f\"💾 Writing data to MinIO: {output_path}\")\n",
    "\n",
    "df.write \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .parquet(output_path)\n",
    "\n",
    "print(\"✅ Data written successfully!\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Read back from MinIO\n",
    "print(\"📖 Reading data from MinIO...\")\n",
    "df_read = spark.read.parquet(output_path)\n",
    "df_read.show()\n",
    "print(\"✅ Data read successfully!\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Clean up\n",
    "spark.stop()\n",
    "print(\"👋 Spark session stopped\")"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.9.0"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
EOF

echo -e "${GREEN}✓ Example files created${NC}"

echo -e "${BLUE}Step 4/5: Creating documentation...${NC}"

# Create .gitignore
cat > .gitignore << 'EOF'
# Data directories - DO NOT commit
postgres/
minio/
nessie/
dremio/
logs/
spark-events/
spark-logs/
spark-work/

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg
.ipynb_checkpoints/

# Environment
.env
.venv
env/
venv/
ENV/
env.bak/
venv.bak/

# IDEs
.vscode/
.idea/
*.swp
*.swo
*~
.project
.pydevproject
.settings/

# OS
.DS_Store
.DS_Store?
._*
.Spotlight-V100
.Trashes
ehthumbs.db
Thumbs.db

# Logs
*.log
EOF

# Create README
cat > README.md << 'EOF'
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

## ⚡ Quick Start

### Prerequisites
- Docker and Docker Compose installed
- At least 16GB RAM available
- 50GB free disk space

### Installation

```bash
# 1. Extract the archive (if received as tar.gz)
tar -xzf data-platform-complete.tar.gz
cd data-platform

# 2. Start all services
docker-compose up -d

# 3. Wait 2-3 minutes for initialization
docker-compose ps

# 4. Access the services (see URLs below)
```

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

- Spark: 3.x
- Airflow: 2.x
- Python: 3.9+
- PostgreSQL: Latest
- MinIO: Latest

## 🎓 Learning Path

1. **Start with Jupyter** - Run `01_getting_started.ipynb`
2. **Create an Airflow DAG** - Modify `example_dag.py`
3. **Write a Spark Job** - Create a new job in `jobs/`
4. **Explore MinIO** - Upload data and query it with Spark
5. **Try Dremio** - Create a data source and run SQL queries

---

**Happy Data Engineering! 🚀**

For questions or issues, check the logs first: `docker-compose logs -f`
EOF

# Create setup instructions for friend
cat > SETUP_INSTRUCTIONS.txt << 'EOF'
╔══════════════════════════════════════════════════════════════╗
║          DATA PLATFORM - SETUP INSTRUCTIONS                  ║
╚══════════════════════════════════════════════════════════════╝

🎯 QUICK START (3 Steps):

1. Extract this archive:
   tar -xzf data-platform-complete.tar.gz
   cd data-platform

2. Start all services:
   docker-compose up -d

3. Wait 2-3 minutes, then access:
   • Airflow:  http://localhost:8085  (admin/admin)
   • Jupyter:  http://localhost:8888  (get token from logs)
   • Spark UI: http://localhost:9090
   • MinIO:    http://localhost:9001  (admin/password)

📋 REQUIREMENTS:
   ✓ Docker & Docker Compose installed
   ✓ 16GB RAM minimum
   ✓ 50GB free disk space

🔍 GET JUPYTER TOKEN:
   docker-compose logs jupyter-spark | grep token

📖 FULL DOCUMENTATION:
   See README.md for complete details

🆘 TROUBLESHOOTING:
   docker-compose logs -f [service-name]

🛑 STOP SERVICES:
   docker-compose down

═══════════════════════════════════════════════════════════════

For detailed information, open README.md in a text editor.
EOF

echo -e "${GREEN}✓ Documentation created${NC}"

echo -e "${BLUE}Step 5/5: Creating shareable package...${NC}"

# Set permissions
chmod -R 777 dags jobs logs notebooks spark-events spark-logs spark-work 2>/dev/null || true

# Create the tar.gz package
tar -czf data-platform-complete.tar.gz \
  docker-compose.yml \
  airflow.env \
  spark-conf/ \
  dags/ \
  jobs/ \
  notebooks/ \
  README.md \
  SETUP_INSTRUCTIONS.txt \
  .gitignore

echo -e "${GREEN}✓ Package created successfully!${NC}"

# Summary
echo ""
echo "=================================================="
echo -e "${GREEN}✅ COMPLETE! Package ready to share${NC}"
echo "=================================================="
echo ""
echo "📦 Package file: data-platform-complete.tar.gz"
echo "📊 Package size: $(du -h data-platform-complete.tar.gz | cut -f1)"
echo ""
echo "📤 Send this file to your friend:"
echo "   → data-platform-complete.tar.gz"
echo ""
echo "📋 Your friend should:"
echo "   1. Extract: tar -xzf data-platform-complete.tar.gz"
echo "   2. Enter:   cd data-platform"
echo "   3. Start:   docker-compose up -d"
echo ""
echo "🌐 Services will be available at:"
echo "   • Airflow:  http://localhost:8085"
echo "   • Jupyter:  http://localhost:8888"
echo "   • Spark:    http://localhost:9090"
echo "   • MinIO:    http://localhost:9001"
echo "   • Dremio:   http://localhost:9047"
echo ""
echo "✨ All examples and documentation included!"
echo ""