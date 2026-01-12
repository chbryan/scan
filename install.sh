#!/bin/bash
set -euo pipefail
trap 'echo "Error on line $LINENO: $BASH_COMMAND"; exit 1' ERR

# Update system
sudo apt update -y && sudo apt upgrade -y || { echo "Failed to update/upgrade system"; exit 1; }

# Install prerequisites
sudo apt install -y git python3 python3-pip chromium chromium-driver postgresql redis-server rabbitmq-server default-jdk apt-transport-https wget gnupg || { echo "Failed to install prerequisites"; exit 1; }

# Install Elasticsearch
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo gpg --dearmor -o /usr/share/keyrings/elasticsearch-keyring.gpg || { echo "Failed to get Elasticsearch key"; exit 1; }
echo "deb [signed-by=/usr/share/keyrings/elasticsearch-keyring.gpg] https://artifacts.elastic.co/packages/8.x/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-8.x.list || { echo "Failed to add Elasticsearch repo"; exit 1; }
sudo apt update -y || { echo "Failed to update after adding repo"; exit 1; }
sudo apt install -y elasticsearch || { echo "Failed to install Elasticsearch"; exit 1; }

# Install Kafka
KAFKA_VERSION=3.6.1
SCALA_VERSION=2.13
wget https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz || { echo "Failed to download Kafka"; exit 1; }
tar -xzf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz || { echo "Failed to extract Kafka"; exit 1; }
sudo mv kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka || { echo "Failed to move Kafka"; exit 1; }
rm kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz

# Create systemd for Zookeeper
cat <<EOF | sudo tee /etc/systemd/system/zookeeper.service || { echo "Failed to create Zookeeper service"; exit 1; }
[Unit]
Description=Apache Zookeeper
After=network.target

[Service]
Type=simple
User=root
ExecStart=/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
ExecStop=/opt/kafka/bin/zookeeper-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
EOF

# Create systemd for Kafka
cat <<EOF | sudo tee /etc/systemd/system/kafka.service || { echo "Failed to create Kafka service"; exit 1; }
[Unit]
Description=Apache Kafka
After=network.target zookeeper.service

[Service]
Type=simple
User=root
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload || { echo "Failed to reload systemd"; exit 1; }

# Enable and start services
for service in postgresql redis-server rabbitmq-server elasticsearch zookeeper kafka; do
  sudo systemctl enable --now $service || { echo "Failed to enable/start $service"; exit 1; }
done

# Setup Postgres
sudo -u postgres psql -c "CREATE USER scan_user WITH PASSWORD 'scan_pass';" || { echo "Failed to create Postgres user"; exit 1; }
sudo -u postgres psql -c "CREATE DATABASE scan_db OWNER scan_user;" || { echo "Failed to create Postgres DB"; exit 1; }
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE scan_db TO scan_user;" || { echo "Failed to grant Postgres privileges"; exit 1; }

# Clone repo
git clone https://github.com/chbryan/scan.git || { echo "Failed to clone repo"; exit 1; }
cd scan || { echo "Failed to cd into scan"; exit 1; }

# Patch scan.py with DB creds
sed -i "s/'your_db'/'scan_db'/g" scan.py || { echo "Failed to patch DB name"; exit 1; }
sed -i "s/'your_user'/'scan_user'/g" scan.py || { echo "Failed to patch DB user"; exit 1; }
sed -i "s/'your_pass'/'scan_pass'/g" scan.py || { echo "Failed to patch DB pass"; exit 1; }

# Install Python dependencies
pip3 install --user scrapy scrapy-selenium scrapy-redis psycopg2-binary elasticsearch kafka-python pika selenium || { echo "Failed to install Python deps"; exit 1; }
