#!/bin/bash

# Setup S3 buckets and folders for file trigger pipeline

echo "🔧 Setting up S3 buckets and folders..."
echo ""

# Create source and target folders in MinIO
docker exec minio-mc /bin/sh -c "
  echo '📂 Creating source folder...'
  /usr/bin/mc mb minio/warehouse/source 2>/dev/null || echo '   (already exists)'
  
  echo '📂 Creating target folder...'
  /usr/bin/mc mb minio/warehouse/target 2>/dev/null || echo '   (already exists)'
  
  echo '✅ Folders created successfully!'
"

# Create sample JSON file
echo ""
echo "📝 Creating sample JSON file..."

cat > sample_data.json << 'EOF'
{"id": 1, "amount": 1000}
{"id": 2, "amount": 2000}
{"id": 3, "amount": 4500}
{"id": 4, "amount": 3200}
{"id": 5, "amount": 5500}
EOF

echo "✅ Sample file created: sample_data.json"
echo ""

# Upload sample file to MinIO source folder
echo "📤 Uploading sample file to MinIO source folder..."
docker exec -i minio-mc /bin/sh -c "
  cat > /tmp/sample_data.json << 'INNEREOF'
{\"id\": 1, \"amount\": 1000}
{\"id\": 2, \"amount\": 2000}
{\"id\": 3, \"amount\": 4500}
{\"id\": 4, \"amount\": 3200}
{\"id\": 5, \"amount\": 5500}
INNEREOF

  /usr/bin/mc cp /tmp/sample_data.json minio/warehouse/source/sample_data.json
  echo '✅ File uploaded successfully!'
"

echo ""
echo "=" * 60
echo "✅ Setup Complete!"
echo "=" * 60
echo ""
echo "📂 Bucket structure:"
echo "   s3a://warehouse/source/  - Input files go here"
echo "   s3a://warehouse/target/  - Output files saved here"
echo ""
echo "🧪 Test the pipeline:"
echo "   1. Go to Airflow UI: http://localhost:8085"
echo "   2. Find DAG: s3_file_processing_pipeline"
echo "   3. Trigger it manually"
echo ""
echo "📤 To upload more files:"
echo "   docker exec minio-mc /usr/bin/mc cp /path/to/file.json minio/warehouse/source/"
echo ""
echo "🌐 View in MinIO Console: http://localhost:9001"
echo "   Username: admin"
echo "   Password: password"
echo ""