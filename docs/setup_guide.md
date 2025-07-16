# AirlineAWS Setup Guide

## Overview

This guide provides step-by-step instructions for setting up the AirlineAWS data engineering pipeline on AWS. The project demonstrates a production-ready, end-to-end data engineering solution for airline choice prediction.

## Prerequisites

### 1. AWS Account Setup
- **AWS Account**: Active AWS account with appropriate permissions
- **AWS CLI**: Installed and configured with access keys
- **IAM Permissions**: Administrator access or equivalent permissions for:
  - CloudFormation
  - S3
  - Redshift
  - Glue
  - Lambda
  - SageMaker
  - Step Functions
  - CloudWatch
  - Secrets Manager
  - IAM

### 2. Development Environment
- **Python**: 3.9 or higher
- **Git**: For version control
- **IDE**: VS Code, PyCharm, or similar
- **Docker**: For containerized development (optional)

### 3. API Keys
- **Aviation Stack API**: Free tier account at [aviationstack.com](https://aviationstack.com/)
- **OpenSky Network**: Free account at [opensky-network.org](https://opensky-network.org/)

## Installation Steps

### Step 1: Clone and Setup Repository

```bash
# Clone the repository
git clone https://github.com/AAJaisiv/AirlineAWS.git
cd AirlineAWS

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Configure Environment Variables

Create a `.env` file in the project root:

```bash
# AWS Configuration
AWS_REGION=us-east-1
AWS_PROFILE=default

# API Keys
AVIATION_STACK_API_KEY=your_aviation_stack_api_key_here
OPENSKY_USERNAME=your_opensky_username_here
OPENSKY_PASSWORD=your_opensky_password_here

# Database Configuration
REDSHIFT_PASSWORD=your_secure_redshift_password_here

# Environment
ENVIRONMENT=development
```

### Step 3: AWS CLI Configuration

```bash
# Configure AWS CLI
aws configure

# Or use a specific profile
aws configure --profile airlineaws
```

### Step 4: Deploy Infrastructure

```bash
# Make deployment script executable
chmod +x scripts/deploy_infrastructure.py

# Deploy infrastructure
python scripts/deploy_infrastructure.py --action deploy --region us-east-1
```

### Step 5: Verify Deployment

```bash
# Check CloudFormation stack status
aws cloudformation describe-stacks --stack-name airlineaws-infrastructure

# List S3 buckets
aws s3 ls | grep airlineaws

# Check Redshift cluster
aws redshift describe-clusters --cluster-identifier airlineaws-cluster
```

## Project Structure

```
AirlineAWS/
├── README.md                           # Project documentation
├── requirements.txt                    # Python dependencies
├── config/
│   └── config.yaml                     # Configuration file
├── infrastructure/
│   └── cloudformation/
│       └── main.yaml                   # CloudFormation template
├── src/
│   ├── data_ingestion/                 # Data ingestion modules
│   ├── data_processing/                # ETL and data processing
│   ├── machine_learning/               # ML models and training
│   ├── orchestration/                  # Airflow DAGs and workflows
│   └── monitoring/                     # Monitoring and alerting
├── notebooks/                          # Jupyter notebooks
├── tests/                             # Unit and integration tests
├── docs/                              # Documentation
├── scripts/                           # Utility scripts
└── .env                               # Environment variables
```

## Configuration

### AWS Services Configuration

The project uses the following AWS services:

1. **S3 Buckets**:
   - `airlineaws-raw-data-{account-id}`: Raw data storage
   - `airlineaws-processed-data-{account-id}`: Processed data
   - `airlineaws-ml-models-{account-id}`: ML models
   - `airlineaws-logs-{account-id}`: Logs and monitoring

2. **Redshift Cluster**:
   - Cluster identifier: `airlineaws-cluster`
   - Database: `airlineaws_db`
   - Node type: `dc2.large` (2 nodes)

3. **Glue**:
   - Database: `airlineaws_glue_db`
   - Crawler: `airlineaws-crawler`
   - ETL Job: `airlineaws-etl-job`

4. **Lambda Functions**:
   - Data ingestion: `airlineaws-data-ingestion`
   - Data processing: `airlineaws-data-processing`
   - Model training: `airlineaws-model-training`

5. **SageMaker**:
   - Role: `AirlineAWS-SageMaker-Role`
   - Notebook: `airlineaws-notebook`
   - Endpoint: `airlineaws-prediction-endpoint`

6. **Step Functions**:
   - State machine: `AirlineAWS-Pipeline`

## Data Pipeline Architecture

### Bronze Layer (Raw Data)
- Raw API responses
- Unprocessed flight data
- Metadata and timestamps
- Data quality metrics

### Silver Layer (Cleaned Data)
- Validated and cleaned data
- Business rules applied
- Data transformations
- Quality checks passed

### Gold Layer (Business Logic)
- Aggregated metrics
- Feature engineering
- ML-ready datasets
- Business insights

## API Integration

### Aviation Stack API
- **Base URL**: `http://api.aviationstack.com/v1`
- **Rate Limit**: 100 requests/month (free tier)
- **Endpoints**:
  - `/flights`: Real-time flight data
  - `/airports`: Airport information
  - `/airlines`: Airline details

### OpenSky Network API
- **Base URL**: `https://opensky-network.org/api`
- **Rate Limit**: 10 requests/second (free tier)
- **Endpoints**:
  - `/states/all`: Aircraft states
  - `/flights/all`: Flight data

## Testing

### Unit Tests
```bash
# Run unit tests
pytest tests/unit/

# Run with coverage
pytest tests/unit/ --cov=src --cov-report=html
```

### Integration Tests
```bash
# Run integration tests
pytest tests/integration/

# Test API connectivity
python src/data_ingestion/aviation_api_client.py
```

### End-to-End Tests
```bash
# Run E2E tests
pytest tests/e2e/

# Test complete pipeline
python scripts/test_pipeline.py
```

## Monitoring and Logging

### CloudWatch
- Log group: `/aws/airlineaws`
- Metrics namespace: `AirlineAWS`
- Retention: 90 days

### Key Metrics
- Data quality scores
- Pipeline performance
- Model accuracy
- API usage statistics

## Troubleshooting

### Common Issues

1. **CloudFormation Deployment Fails**
   ```bash
   # Check stack events
   aws cloudformation describe-stack-events --stack-name airlineaws-infrastructure
   
   # Validate template
   aws cloudformation validate-template --template-body file://infrastructure/cloudformation/main.yaml
   ```

2. **API Rate Limiting**
   - Check API usage in Aviation Stack dashboard
   - Implement exponential backoff in API client
   - Monitor rate limit headers

3. **Redshift Connection Issues**
   ```bash
   # Check cluster status
   aws redshift describe-clusters --cluster-identifier airlineaws-cluster
   
   # Test connection
   psql -h <cluster-endpoint> -U admin -d airlineaws_db
   ```

4. **S3 Permission Errors**
   - Verify IAM roles and policies
   - Check bucket permissions
   - Ensure proper ARN formatting

### Debug Mode

Enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Security Best Practices

1. **IAM Roles**: Use least privilege principle
2. **Secrets Management**: Store sensitive data in AWS Secrets Manager
3. **Encryption**: Enable encryption for all data at rest and in transit
4. **Network Security**: Use VPC and security groups
5. **Access Control**: Implement proper access controls and monitoring

## Cost Optimization

### Development Environment
- Use smaller instance types
- Schedule non-production resources to stop during off-hours
- Monitor usage with AWS Cost Explorer

### Production Environment
- Use reserved instances for predictable workloads
- Implement auto-scaling policies
- Monitor and optimize data storage

## Next Steps

1. **Data Collection**: Start collecting flight data using the API client
2. **Model Development**: Develop and train ML models
3. **Pipeline Orchestration**: Set up Airflow DAGs
4. **Monitoring**: Implement comprehensive monitoring
5. **Documentation**: Create detailed technical documentation

## Support

For issues and questions:
- Check the troubleshooting section
- Review AWS documentation
- Create an issue in the GitHub repository

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

---

**Note**: This setup guide assumes a development environment. For production deployment, additional security, monitoring, and scaling considerations should be implemented. 