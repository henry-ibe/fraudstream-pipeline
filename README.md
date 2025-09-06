
Real-time(ish) fraud detection pipeline:
- Producer sends synthetic transactions to **SQS**
- Consumer (FastAPI) scores and **dual-writes**: **PostgreSQL (RDS)** + **S3**
- Query S3 with **Athena**; live dashboards in **Grafana**
- Deployed on **k3s** (single-node Kubernetes) on EC2

## Structure
- `producer/` — SQS transaction generator
- `fraud-consumer/app/main.py` — scoring service
- `fraud-consumer/Dockerfile`
- `fraud-consumer/k8s/` — K8s manifests (Deployment/Service/etc.)

> Built to run almost entirely on the AWS Free Tier.

