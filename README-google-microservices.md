# Google Microservices Deployment Guide

This guide covers the end-to-end setup for running the **Google Microservices Demo** with **Prometheus-Sketches** and **PromSketch** integrations across three CloudLab nodes.

---

## 1. SSH Setup

Generate an SSH key on each node and add the public key where required (e.g., GitHub, other nodes).

```bash
ssh-keygen
cat ~/.ssh/id_rsa.pub
```

---

## 2. Base System Bootstrap

Run the following script on every node to install Docker, Python tooling, and Go.

```bash
sudo apt update && sudo apt install docker.io python3-pip -y

pip install prometheus_client pandas matplotlib

# Install Go 1.25.1
wget https://go.dev/dl/go1.25.1.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.25.1.linux-amd64.tar.gz

echo "export PATH=\$PATH:/usr/local/go/bin" >> ~/.bashrc
echo "export TMPDIR=/mydata" >> ~/.bashrc
source ~/.bashrc
```

Mount and prepare the extra disk:

```bash
sudo mkdir /mydata
sudo /usr/local/etc/emulab/mkextrafs.pl /mydata
cd /mydata
sudo chmod -R 777 ./
```

---

## 3. Node 0 – Google Microservices + Prometheus-Sketches

* SSH: `ssh siedeta@c220g2-011010.wisc.cloudlab.us`
* Prometheus UI: `http://192.168.49.2:30900/`

### 3.1 Minikube and Kubernetes Tooling

```bash
# Install Minikube
curl -LO https://github.com/kubernetes/minikube/releases/latest/download/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube
rm minikube-linux-amd64

# Install kubectl (follow the latest instructions)
curl -LO https://dl.k8s.io/release/v1.30.0/bin/linux/amd64/kubectl
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
rm kubectl

sudo systemctl start docker
sudo systemctl enable docker
systemctl status docker
sudo usermod -aG docker $USER
```

Log out and back in (or run `newgrp docker`) to apply the Docker group membership.

### 3.2 Deploy the Demo

```bash
git clone https://github.com/GoogleCloudPlatform/microservices-demo.git
cd microservices-demo

minikube start --cpus=4 --memory=4096 --disk-size=32g

kubectl apply -f release/kubernetes-manifests.yaml
kubectl get nodes
kubectl get pods
```

Forward the frontend service for validation:

```bash
kubectl port-forward deployment/frontend 8080:8080
ssh -N -L 18080:127.0.0.1:8080 siedeta@c220g2-011010.wisc.cloudlab.us
# Open http://localhost:18080/ locally
```

### 3.3 Configure Git for Private Modules

```bash
go env -w GOPRIVATE=github.com/zzylol/*
git config --global url."ssh://git@github.com/".insteadOf "https://github.com/"
```

### 3.4 Add Official Prometheus to the Manifests

Update `release/kubernetes-manifests.yaml` to include the original Prometheus image provided at <https://github.com/copilot/c/6225af1c-b67e-4e82-ba49-6ecaf0e713fc>. Deploy Prometheus with the Kubernetes data collector and metrics enabled, then verify metrics in the Prometheus UI.

Restart Prometheus when changes are applied:

```bash
kubectl rollout restart deploy/prometheus
kubectl rollout status deploy/prometheus
```

### 3.5 Build and Push Prometheus-Sketches Image

Use the Dockerfile below (from `prometheus-sketches`) to build the customized Prometheus image.

```Dockerfile
# syntax=docker/dockerfile:1
FROM golang:1.25 AS builder
WORKDIR /src

COPY go.mod go.sum ./
COPY vendor ./vendor
ENV GOFLAGS="-mod=vendor"
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/prometheus ./cmd/prometheus

FROM gcr.io/distroless/base-debian12
COPY --from=builder /out/prometheus /bin/prometheus
ENTRYPOINT ["/bin/prometheus"]
```

Push the image to the private registry on Node 0:

```bash
export NODE0_IP=10.10.1.1
sudo docker rm -f registry || true
sudo docker run -d --name registry --restart=always -p 9090:5000 registry:2

sudo docker tag prometheus-sketches:local ${NODE0_IP}:9090/prometheus-sketches:v0.1
echo '{ "insecure-registries": ["'${NODE0_IP}':9090"] }' | sudo tee /etc/docker/daemon.json
sudo systemctl restart docker
sudo docker push ${NODE0_IP}:9090/prometheus-sketches:v0.1
```

Load the image into Minikube and roll out the updated deployment:

```bash
docker pull 10.10.1.1:9090/prometheus-sketches:v0.1
minikube image load 10.10.1.1:9090/prometheus-sketches:v0.1
kubectl delete pod -l app=prometheus
kubectl rollout status deploy/prometheus
```

### 3.7 Apply Monitoring Resources

```bash
kubectl apply -f kubernetes-manifests/
kubectl apply -f monitoring.yaml
```

### 3.6 Port-Forwarding Prometheus

```bash
kubectl -n monitoring port-forward svc/prometheus 9090:9090
ssh -N -L 9090:127.0.0.1:9090 siedeta@c220g2-011331.wisc.cloudlab.us
curl http://127.0.0.1:9090/
```

---

## 4. Node 1 – Grafana

* SSH: `ssh siedeta@c220g2-011029.wisc.cloudlab.us`

### 4.1 Install and Run Grafana

```bash
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

docker run -d --name grafana --network=host \
  -e GF_SECURITY_ADMIN_PASSWORD='admin' \
  -v grafana-data:/var/lib/grafana \
  grafana/grafana:10.4.0
```

### 4.2 Tunnels for Prometheus Access

```bash
ssh -N -L 30900:192.168.49.2:30900 siedeta@c220g2-011010.wisc.cloudlab.us
curl -sf http://127.0.0.1:30900/-/ready && echo OK
```

From your local workstation:

```bash
ssh -N \
  -L 3000:10.10.1.2:3000 \
  -L 9090:192.168.49.2:30900 \
  siedeta@c220g2-011010.wisc.cloudlab.us

# Grafana UI: http://localhost:3000/
# Prometheus UI: http://localhost:9090/
```

---

## 5. Node 2 – PromSketch Standalone & Kafka

* SSH: `ssh siedeta@c220g2-011308.wisc.cloudlab.us`
* Set private module access: `go env -w GOPRIVATE=github.com/zzylol/*,github.com/SieDeta/*`

### 5.1 Kafka (Single-Node via Docker Compose)

Create `docker-compose.yml`:

```yaml
services:
  kafka:
    image: soldevelo/kafka:4.2
    container_name: kafka-single
    restart: unless-stopped
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      - KAFKA_ENABLE_KRAFT=yes
      - KAFKA_CFG_PROCESS_ROLES=broker,controller
      - KAFKA_CFG_NODE_ID=1
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      - ALLOW_PLAINTEXT_LISTENER=yes
      - KAFKA_KRAFT_CLUSTER_ID=abcdefghijklmnopqrstuv
    volumes:
      - kafka_data:/bitnami/kafka

volumes:
  kafka_data:
```

Manage the stack:

```bash
docker compose down -v
docker compose up -d
docker compose logs -f
```

Look for the log line: `[KAFKA] Producer ready (topic=promsketch.metrics, brokers=127.0.0.1:9092)`.

### 5.2 PromSketch → Kafka

```bash
export KAFKA_BROKERS="127.0.0.1:9092"
export KAFKA_TOPIC="promsketch.metrics"
./promsketch    # or: go run main.go
```

### 5.3 Tunnel Kafka to Grafana (Node 1)

```bash
ssh -N -L 9092:127.0.0.1:9092 siedeta@c220g2-011308.wisc.cloudlab.us
nc -vz localhost 9092    # should succeed on Node 1
```

---

## 6. Grafana Data Sources

1. **Prometheus**  
   * URL: `http://localhost:30900` (from Node 1)
   * Validate with `curl -sf http://127.0.0.1:30900/-/ready`.

2. **Kafka (promsketch-std-kafka-datasource plugin)**  
   * Bootstrap Servers: `localhost:9092`  
   * Panel configuration:  
     * Topic: `promsketch.metrics`  
     * Timestamp field (ms): `timestamp`  
     * Value field: `value`  
     * Optional labels: `name`, `labels.instance`, `labels.cpu`, etc.

Ensure both data sources can be queried simultaneously within Grafana.

---

## 7. Prometheus & PromSketch Validation

Port-forward Prometheus services as needed:

```bash
kubectl -n monitoring port-forward svc/prometheus 9090:9090
curl -sf http://127.0.0.1:30900/-/ready && echo OK
```

Example Prometheus queries:

* `sum(rate(prometheus_tsdb_head_samples_appended_total[5m]))` – ingestion rate.
* `prometheus_tsdb_head_series` – active series count.

Example PromSketch queries (via `python3 custom_ingester_noDB_test3_dynamic.py --config=scraper-config.yml`):

```bash
curl -sG "http://127.0.0.1:7001/parse" \
  --data-urlencode 'q=avg_over_time(node_memory_MemAvailable_bytes[300s])'
```

Sample output:

```
=== Running Rule: avg_over_time ===
[LOCAL ] Query latency : 4.75 ms
[SERVER] Query latency : 0.01 ms
[RESULT] fake_machine_metric{machineid="machine_0"} = 160337422080 @ 1759157694906
```

Additional rules: `min_over_time`, `max_over_time`, `stddev_over_time`, `quantile_over_time(0.50)`, etc., produce similar telemetry for validation.

---

## 8. Troubleshooting Checklist

* Verify tunnels (`ssh -N -L ...`) are active for Prometheus and Kafka endpoints.
* Ensure `minikube image load` has been run before restarting Prometheus pods.
* Confirm Grafana plugins (e.g., `promsketch-std-kafka-datasource`) are installed if custom data sources are missing.
* If Prometheus pods fail, inspect with `kubectl logs deploy/prometheus` and `kubectl describe pod`.
* For Kafka connectivity, re-check `docker compose logs -f` and confirm no port conflicts.

---

By following this sequence on Nodes 0–2, you will have the Google Microservices Demo integrated with Prometheus-Sketches, PromSketch, and Grafana dashboards backed by both Prometheus and Kafka data sources.
