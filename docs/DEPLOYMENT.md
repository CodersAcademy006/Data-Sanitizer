# Data Sanitizer: Deployment & Infrastructure Guide

## Overview

This guide covers deploying Data Sanitizer to a production environment using:
- **IaC**: Terraform for AWS infrastructure
- **Containerization**: Docker for microservices
- **Orchestration**: Kubernetes (EKS) for autoscaling and resilience
- **GitOps**: ArgoCD for declarative deployment

---

## 1. Infrastructure as Code (Terraform)

### Directory Structure

```
infrastructure/
├── terraform/
│   ├── main.tf              # Main resource definitions
│   ├── variables.tf         # Input variables
│   ├── outputs.tf           # Output values
│   ├── postgres.tf          # RDS Postgres
│   ├── milvus.tf            # Milvus deployment
│   ├── s3.tf                # S3 buckets
│   ├── redis.tf             # ElastiCache Redis
│   ├── eks.tf               # EKS cluster
│   ├── iam.tf               # IAM roles & policies
│   └── monitoring.tf        # CloudWatch, Prometheus
├── terraform.tfvars         # Environment-specific values
└── README.md
```

### Key Resources

#### PostgreSQL (RDS)

```hcl
# postgres.tf
resource "aws_db_instance" "main" {
  identifier             = "data-sanitizer-db"
  allocated_storage      = 100
  engine                 = "postgres"
  engine_version         = "14.7"
  instance_class         = "db.t3.large"
  username               = var.db_username
  password               = var.db_password
  publicly_accessible    = false
  
  backup_retention_period = 30
  backup_window          = "03:00-04:00"
  maintenance_window     = "sun:04:00-sun:05:00"
  
  multi_az               = true
  
  storage_encrypted      = true
  kms_key_id             = aws_kms_key.rds.arn
  
  performance_insights_enabled = true
  
  tags = {
    Name = "data-sanitizer-db"
    Environment = var.environment
  }
}

# Read replica for reporting
resource "aws_db_instance" "read_replica" {
  identifier = "data-sanitizer-db-read"
  replicate_source_db = aws_db_instance.main.identifier
  
  skip_final_snapshot = false
  final_snapshot_identifier = "data-sanitizer-db-read-final-snapshot"
}
```

#### S3 Buckets

```hcl
# s3.tf
resource "aws_s3_bucket" "artifacts" {
  bucket = "data-sanitizer-artifacts-${var.environment}"
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Lifecycle policy: archive old files
resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  
  rule {
    id = "archive-old-data"
    status = "Enabled"
    
    transitions {
      days = 90
      storage_class = "GLACIER"
    }
  }
}
```

#### EKS Cluster

```hcl
# eks.tf
resource "aws_eks_cluster" "main" {
  name            = "data-sanitizer-${var.environment}"
  version         = "1.27"
  role_arn        = aws_iam_role.eks_service_role.arn
  
  vpc_config {
    subnet_ids              = var.subnet_ids
    security_group_ids      = [aws_security_group.eks.id]
    endpoint_private_access = true
    endpoint_public_access  = true
  }
  
  enabled_cluster_log_types = ["api", "audit", "authenticator", "controllerManager", "scheduler"]
  
  tags = {
    Environment = var.environment
  }
}

# Worker nodes (on-demand + spot mix)
resource "aws_eks_node_group" "on_demand" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "on-demand"
  node_role_arn   = aws_iam_role.eks_node_role.arn
  subnet_ids      = var.subnet_ids
  
  capacity_type = "ON_DEMAND"
  disk_size     = 100
  
  scaling_config {
    desired_size = 3
    min_size     = 2
    max_size     = 10
  }
  
  instance_types = ["m5.xlarge"]
  
  tags = {
    Environment = var.environment
  }
}

resource "aws_eks_node_group" "spot" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "spot"
  node_role_arn   = aws_iam_role.eks_node_role.arn
  subnet_ids      = var.subnet_ids
  
  capacity_type = "SPOT"
  disk_size     = 100
  
  scaling_config {
    desired_size = 2
    min_size     = 0
    max_size     = 10
  }
  
  instance_types = ["m5.xlarge", "m5a.xlarge", "m6i.xlarge"]
  
  tags = {
    Environment = var.environment
  }
}
```

---

## 2. Docker Containerization

### Dockerfile Structure

```
docker/
├── api/Dockerfile
├── worker-pass1/Dockerfile
├── worker-pass2/Dockerfile
├── ui/Dockerfile
└── .dockerignore
```

#### Example: API Server Dockerfile

```dockerfile
# docker/api/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY api_server.py .
COPY storage_backend.py .
COPY data_cleaning.py .

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/api/v1/health')"

# Run server
CMD ["uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "8000"]
```

#### Example: Worker Dockerfile

```dockerfile
# docker/worker-pass1/Dockerfile
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data_cleaning.py .
COPY storage_backend.py .
COPY cloud_storage.py .
COPY worker_pass1.py .

CMD ["python", "worker_pass1.py"]
```

### Building & Pushing Images

```bash
# Build images
docker build -t data-sanitizer:api-1.0.0 -f docker/api/Dockerfile .
docker build -t data-sanitizer:worker-pass1-1.0.0 -f docker/worker-pass1/Dockerfile .

# Push to ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 123456789.dkr.ecr.us-east-1.amazonaws.com

docker tag data-sanitizer:api-1.0.0 123456789.dkr.ecr.us-east-1.amazonaws.com/data-sanitizer:api-1.0.0
docker push 123456789.dkr.ecr.us-east-1.amazonaws.com/data-sanitizer:api-1.0.0
```

---

## 3. Kubernetes Manifests

### Directory Structure

```
k8s/
├── kustomization.yaml
├── overlays/
│   ├── dev/
│   │   ├── kustomization.yaml
│   │   └── patches/
│   ├── staging/
│   │   ├── kustomization.yaml
│   │   └── patches/
│   └── prod/
│       ├── kustomization.yaml
│       └── patches/
├── base/
│   ├── api-deployment.yaml
│   ├── api-service.yaml
│   ├── worker-pass1-deployment.yaml
│   ├── worker-pass2-deployment.yaml
│   ├── configmap.yaml
│   ├── secret.yaml
│   ├── pdb.yaml
│   └── hpa.yaml
```

### API Deployment (Base)

```yaml
# k8s/base/api-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: data-sanitizer-api
  labels:
    app: data-sanitizer-api
    version: v1
spec:
  replicas: 3
  selector:
    matchLabels:
      app: data-sanitizer-api
  template:
    metadata:
      labels:
        app: data-sanitizer-api
    spec:
      containers:
      - name: api
        image: 123456789.dkr.ecr.us-east-1.amazonaws.com/data-sanitizer:api-1.0.0
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: 8000
          name: http
          protocol: TCP
        
        # Environment variables from ConfigMap & Secrets
        envFrom:
        - configMapRef:
            name: data-sanitizer-config
        - secretRef:
            name: data-sanitizer-secrets
        
        # Resource requests & limits
        resources:
          requests:
            cpu: 500m
            memory: 512Mi
          limits:
            cpu: 2000m
            memory: 2Gi
        
        # Health checks
        livenessProbe:
          httpGet:
            path: /api/v1/health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
        
        readinessProbe:
          httpGet:
            path: /api/v1/health
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
          timeoutSeconds: 3
          failureThreshold: 2
        
        # Volume mounts (if needed)
        volumeMounts:
        - name: tmp
          mountPath: /tmp
      
      volumes:
      - name: tmp
        emptyDir: {}
      
      # Pod Disruption Budget
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              labelSelector:
                matchExpressions:
                - key: app
                  operator: In
                  values:
                  - data-sanitizer-api
              topologyKey: kubernetes.io/hostname
```

### Horizontal Pod Autoscaler

```yaml
# k8s/base/hpa.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: data-sanitizer-api-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: data-sanitizer-api
  minReplicas: 3
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
    scaleUp:
      stabilizationWindowSeconds: 0
      policies:
      - type: Percent
        value: 100
        periodSeconds: 30
      - type: Pods
        value: 5
        periodSeconds: 30
      selectPolicy: Max
```

### ConfigMap for Environment

```yaml
# k8s/base/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: data-sanitizer-config
data:
  PG_HOST: data-sanitizer-db.c123abc.rds.amazonaws.com
  PG_PORT: "5432"
  PG_DB: data_sanitizer
  MILVUS_HOST: milvus.milvus
  MILVUS_PORT: "19530"
  REDIS_HOST: data-sanitizer-redis.abc123.ng.0001.use1.cache.amazonaws.com
  LOG_LEVEL: INFO
```

### Secrets (encrypted with SOPS)

```yaml
# k8s/base/secret.yaml (encrypted with SOPS)
apiVersion: v1
kind: Secret
metadata:
  name: data-sanitizer-secrets
type: Opaque
stringData:
  PG_USER: postgres
  PG_PASSWORD: <encrypted>
  AWS_ACCESS_KEY_ID: <encrypted>
  AWS_SECRET_ACCESS_KEY: <encrypted>
  OPENAI_API_KEY: <encrypted>
```

---

## 4. GitOps with ArgoCD

### ArgoCD Application

```yaml
# argocd/data-sanitizer-app.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: data-sanitizer
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/org/data-sanitizer
    targetRevision: main
    path: k8s/overlays/prod
  destination:
    server: https://kubernetes.default.svc
    namespace: default
  
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
    - CreateNamespace=true
  
  # Notification: notify on sync
  notifications:
    - type: slack
      when: sync-succeeded, sync-failed
```

---

## 5. Monitoring & Observability

### Prometheus Stack (Helm)

```bash
# Install Prometheus, Grafana, etc.
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

helm install prometheus prometheus-community/kube-prometheus-stack \
  -n monitoring --create-namespace \
  -f prometheus-values.yaml
```

### ServiceMonitor for Data Sanitizer

```yaml
# k8s/monitoring/servicemonitor.yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: data-sanitizer-api
  labels:
    app: data-sanitizer-api
spec:
  selector:
    matchLabels:
      app: data-sanitizer-api
  endpoints:
  - port: http
    path: /metrics
    interval: 30s
```

---

## 6. CI/CD Pipeline (GitHub Actions)

```yaml
# .github/workflows/deploy.yaml
name: Build and Deploy

on:
  push:
    branches: [main]

env:
  AWS_REGION: us-east-1
  ECR_REPOSITORY: data-sanitizer

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:14
        env:
          POSTGRES_DB: test_db
          POSTGRES_PASSWORD: test
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    steps:
    - uses: actions/checkout@v3
    - uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: pip install -r requirements.txt
    
    - name: Run tests
      run: pytest tests.py --cov=. --cov-report=xml
    
    - name: Upload coverage
      uses: codecov/codecov-action@v3

  build-and-push:
    needs: test
    runs-on: ubuntu-latest
    permissions:
      id-token: write
      contents: read
    steps:
    - uses: actions/checkout@v3
    
    - name: Configure AWS credentials
      uses: aws-actions/configure-aws-credentials@v2
      with:
        role-to-assume: arn:aws:iam::${{ secrets.AWS_ACCOUNT_ID }}:role/github-actions
        aws-region: ${{ env.AWS_REGION }}
    
    - name: Login to Amazon ECR
      id: login-ecr
      uses: aws-actions/amazon-ecr-login@v1
    
    - name: Build, tag, and push Docker images
      env:
        ECR_REGISTRY: ${{ steps.login-ecr.outputs.registry }}
      run: |
        docker build -t $ECR_REGISTRY/$ECR_REPOSITORY:api-$GITHUB_SHA -f docker/api/Dockerfile .
        docker push $ECR_REGISTRY/$ECR_REPOSITORY:api-$GITHUB_SHA
        
        docker build -t $ECR_REGISTRY/$ECR_REPOSITORY:worker-pass1-$GITHUB_SHA -f docker/worker-pass1/Dockerfile .
        docker push $ECR_REGISTRY/$ECR_REPOSITORY:worker-pass1-$GITHUB_SHA
    
    - name: Update Kubernetes manifests
      run: |
        # Update image tag in k8s manifests
        sed -i "s|:latest|:$GITHUB_SHA|g" k8s/overlays/prod/api-deployment.yaml
        git config user.name "GitHub Actions"
        git config user.email "actions@github.com"
        git commit -am "Update images to $GITHUB_SHA"
        git push
    
    - name: Notify Slack
      if: failure()
      uses: slackapi/slack-github-action@v1.24.0
      with:
        payload: |
          {
            "text": "Build failed: ${{ github.repository }} ${{ github.ref }}"
          }
```

---

## 7. Deployment Checklist

- [ ] Terraform plan & apply (infrastructure)
- [ ] RDS Postgres initialized with schema migrations
- [ ] S3 buckets created with versioning & encryption
- [ ] EKS cluster created with worker nodes
- [ ] Docker images built and pushed to ECR
- [ ] Kubernetes manifests (ConfigMaps, Secrets, Deployments)
- [ ] ArgoCD synced with GitOps repository
- [ ] Prometheus & Grafana dashboards configured
- [ ] SSL/TLS certificates (AWS Certificate Manager)
- [ ] Route53 DNS records
- [ ] Backups & disaster recovery plan tested
- [ ] Security scan completed (IAM, VPC, encryption)
- [ ] Load testing & SLA validation

---

## 8. Operational Runbooks

### Scaling Workers

```bash
# Manually scale worker deployment
kubectl scale deployment data-sanitizer-pass1-worker --replicas=10 -n default

# Check HPA status
kubectl get hpa data-sanitizer-pass1-worker-hpa -w
```

### Database Backup & Restore

```bash
# Backup RDS to S3
aws rds create-db-snapshot --db-instance-identifier data-sanitizer-db \
  --db-snapshot-identifier data-sanitizer-db-backup-$(date +%Y%m%d)

# Restore from snapshot
aws rds restore-db-instance-from-db-snapshot \
  --db-instance-identifier data-sanitizer-db-restored \
  --db-snapshot-identifier data-sanitizer-db-backup-20231116
```

### View Logs

```bash
# API logs
kubectl logs -f deployment/data-sanitizer-api

# Worker logs
kubectl logs -f deployment/data-sanitizer-pass1-worker -l app=data-sanitizer

# Describe pod for events
kubectl describe pod <pod-name>
```

---

## 9. Cost Optimization

### Reserved Instances
- Purchase 1-year RDS Reserved Instance (30% discount)
- Purchase 3-year EKS Compute Reserved Instances

### Spot Instances
- Use 40% spot nodes for worker pools (up to 70% savings)
- Set interruption handling with Pod Disruption Budgets

### Data Tiering
- Archive old cleaned datasets to S3 Glacier (90 days)
- Delete temporary files from job staging

### Resource Right-Sizing
- Monitor actual usage vs. requests/limits
- Adjust CPU/memory based on metrics

---

## References

- Terraform AWS Provider: https://registry.terraform.io/providers/hashicorp/aws/latest/docs
- Kubernetes Documentation: https://kubernetes.io/docs
- ArgoCD: https://argo-cd.readthedocs.io/
- EKS Best Practices: https://aws.github.io/aws-eks-best-practices/
