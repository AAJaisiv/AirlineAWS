#!/usr/bin/env python3
"""
Quick Start Script for AirlineAWS

This script provides a guided setup process for the AirlineAWS project,
helping users get started quickly with the data engineering pipeline.

Author: A Abhinav Jaisiv
Date: 2025
"""

import os
import sys
import subprocess
import logging
from pathlib import Path
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class QuickStart:
    """
    Quick start guide for AirlineAWS project setup.
    """
    
    def __init__(self):
        """Initialize the quick start process."""
        self.project_root = Path(__file__).parent.parent
        self.setup_complete = False
        
    def print_banner(self):
        """Print project banner."""
        banner = """
╔══════════════════════════════════════════════════════════════╗
║                    AirlineAWS Quick Start                    ║
║                                                              ║
║  End-to-End Data Engineering Pipeline for Airline Analytics ║
║                                                              ║
║  By: A Abhinav Jaisiv                                        ║
║  GitHub: https://github.com/AAJaisiv/AirlineAWS             ║
╚══════════════════════════════════════════════════════════════╝
        """
        print(banner)
    
    def check_prerequisites(self) -> bool:
        """Check if all prerequisites are met."""
        logger.info("Checking prerequisites...")
        
        # Check Python version
        if sys.version_info < (3, 9):
            logger.error("Python 3.9 or higher is required")
            return False
        
        # Check if AWS CLI is installed
        try:
            subprocess.run(['aws', '--version'], capture_output=True, check=True)
            logger.info("✓ AWS CLI is installed")
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.warning("⚠ AWS CLI not found. Please install it from: https://aws.amazon.com/cli/")
            return False
        
        # Check if git is installed
        try:
            subprocess.run(['git', '--version'], capture_output=True, check=True)
            logger.info("✓ Git is installed")
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.warning("⚠ Git not found. Please install it from: https://git-scm.com/")
            return False
        
        logger.info("✓ All prerequisites are met")
        return True
    
    def setup_virtual_environment(self) -> bool:
        """Set up Python virtual environment."""
        logger.info("Setting up virtual environment...")
        
        venv_path = self.project_root / "venv"
        
        if venv_path.exists():
            logger.info("✓ Virtual environment already exists")
            return True
        
        try:
            subprocess.run([sys.executable, '-m', 'venv', 'venv'], 
                         cwd=self.project_root, check=True)
            logger.info("✓ Virtual environment created successfully")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to create virtual environment: {e}")
            return False
    
    def install_dependencies(self) -> bool:
        """Install Python dependencies."""
        logger.info("Installing dependencies...")
        
        requirements_file = self.project_root / "requirements.txt"
        if not requirements_file.exists():
            logger.error("requirements.txt not found")
            return False
        
        try:
            # Determine the correct pip command
            if os.name == 'nt':  # Windows
                pip_cmd = str(self.project_root / "venv" / "Scripts" / "pip")
            else:  # Unix/Linux/macOS
                pip_cmd = str(self.project_root / "venv" / "bin" / "pip")
            
            subprocess.run([pip_cmd, 'install', '-r', 'requirements.txt'], 
                         cwd=self.project_root, check=True)
            logger.info("✓ Dependencies installed successfully")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to install dependencies: {e}")
            return False
    
    def create_env_file(self) -> bool:
        """Create .env file with required environment variables."""
        logger.info("Creating .env file...")
        
        env_file = self.project_root / ".env"
        if env_file.exists():
            logger.info("✓ .env file already exists")
            return True
        
        env_template = """# AirlineAWS Environment Configuration

# AWS Configuration
AWS_REGION=us-east-1
AWS_PROFILE=default

# API Keys (Get these from the respective websites)
AVIATION_STACK_API_KEY=your_aviation_stack_api_key_here
OPENSKY_USERNAME=your_opensky_username_here
OPENSKY_PASSWORD=your_opensky_password_here

# Database Configuration
REDSHIFT_PASSWORD=your_secure_redshift_password_here

# Environment
ENVIRONMENT=development

# Optional: Override default settings
# LOG_LEVEL=INFO
# DATA_PROCESSING_BATCH_SIZE=1000
"""
        
        try:
            with open(env_file, 'w') as f:
                f.write(env_template)
            logger.info("✓ .env file created successfully")
            logger.info("⚠ Please update the .env file with your API keys and credentials")
            return True
        except Exception as e:
            logger.error(f"Failed to create .env file: {e}")
            return False
    
    def validate_aws_configuration(self) -> bool:
        """Validate AWS configuration."""
        logger.info("Validating AWS configuration...")
        
        try:
            # Check if AWS credentials are configured
            result = subprocess.run(['aws', 'sts', 'get-caller-identity'], 
                                  capture_output=True, text=True, check=True)
            logger.info("✓ AWS credentials are configured")
            logger.info(f"✓ AWS Account: {result.stdout}")
            return True
        except subprocess.CalledProcessError as e:
            logger.error("AWS credentials not configured or invalid")
            logger.info("Please run: aws configure")
            return False
    
    def test_api_connectivity(self) -> bool:
        """Test API connectivity (optional)."""
        logger.info("Testing API connectivity...")
        
        # Check if API keys are configured
        env_file = self.project_root / ".env"
        if not env_file.exists():
            logger.warning("⚠ .env file not found, skipping API test")
            return True
        
        try:
            # Load environment variables
            with open(env_file, 'r') as f:
                for line in f:
                    if '=' in line and not line.startswith('#'):
                        key, value = line.strip().split('=', 1)
                        os.environ[key] = value
            
            # Test API client
            sys.path.append(str(self.project_root / "src"))
            from data_ingestion.aviation_api_client import AviationAPIClient
            
            client = AviationAPIClient()
            stats = client.get_api_usage_stats()
            logger.info("✓ API client initialized successfully")
            logger.info(f"✓ API Configuration: {stats}")
            return True
            
        except Exception as e:
            logger.warning(f"⚠ API test failed: {e}")
            logger.info("This is expected if API keys are not configured yet")
            return True
    
    def print_next_steps(self):
        """Print next steps for the user."""
        next_steps = """
╔══════════════════════════════════════════════════════════════╗
║                        Next Steps                            ║
╚══════════════════════════════════════════════════════════════╝

1. 📝 Configure API Keys:
   • Get Aviation Stack API key: https://aviationstack.com/
   • Get OpenSky Network credentials: https://opensky-network.org/
   • Update the .env file with your credentials

2. 🚀 Deploy Infrastructure:
   • Run: python scripts/deploy_infrastructure.py --action deploy
   • This will create all AWS resources

3. 📊 Start Data Collection:
   • Run: python src/data_ingestion/aviation_api_client.py
   • This will test API connectivity and collect sample data

4. 📚 Read Documentation:
   • Setup Guide: docs/setup_guide.md
   • Project Summary: docs/project_summary.md
   • API Research: docs/airline_apis_research.md

5. 🔧 Development:
   • Activate virtual environment: source venv/bin/activate
   • Run tests: pytest tests/
   • Start development server: python -m src.main

6. 📈 Monitor Pipeline:
   • Check CloudWatch logs: /aws/airlineaws
   • Monitor S3 buckets for data
   • Review Redshift cluster status

╔══════════════════════════════════════════════════════════════╗
║                    Useful Commands                           ║
╚══════════════════════════════════════════════════════════════╝

• Deploy infrastructure: python scripts/deploy_infrastructure.py --action deploy
• Clean up infrastructure: python scripts/deploy_infrastructure.py --action cleanup --stack-name airlineaws-infrastructure
• Test API client: python src/data_ingestion/aviation_api_client.py
• Run tests: pytest tests/
• Check AWS resources: aws cloudformation describe-stacks --stack-name airlineaws-infrastructure

╔══════════════════════════════════════════════════════════════╗
║                      Support                                 ║
╚══════════════════════════════════════════════════════════════╝

• GitHub: https://github.com/AAJaisiv/AirlineAWS
• Documentation: docs/
• Issues: Create an issue on GitHub
• Email: [Your Email]

Happy coding! 🚀
        """
        print(next_steps)
    
    def run_setup(self) -> bool:
        """Run the complete setup process."""
        self.print_banner()
        
        steps = [
            ("Checking prerequisites", self.check_prerequisites),
            ("Setting up virtual environment", self.setup_virtual_environment),
            ("Installing dependencies", self.install_dependencies),
            ("Creating .env file", self.create_env_file),
            ("Validating AWS configuration", self.validate_aws_configuration),
            ("Testing API connectivity", self.test_api_connectivity),
        ]
        
        for step_name, step_func in steps:
            logger.info(f"\n{'='*60}")
            logger.info(f"Step: {step_name}")
            logger.info(f"{'='*60}")
            
            if not step_func():
                logger.error(f"Setup failed at step: {step_name}")
                return False
        
        self.setup_complete = True
        logger.info(f"\n{'='*60}")
        logger.info("🎉 Setup completed successfully!")
        logger.info(f"{'='*60}")
        
        self.print_next_steps()
        return True


def main():
    """Main function for quick start script."""
    try:
        quick_start = QuickStart()
        success = quick_start.run_setup()
        
        if success:
            sys.exit(0)
        else:
            logger.error("Setup failed. Please check the errors above.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("\nSetup interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 