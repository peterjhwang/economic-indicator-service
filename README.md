# economic-indicator-service

This is a Flask backend as well as orchestrator for the economic-indicator project. It returns Plotly object for the data visualisation.

Deployed to AWS elastic beanstalk as a Flask application.

## What it does?
1. Backend for the economic-indicator project frontend. It returns Plotly object for the data visualisation.
2. Pulls all stats data via Stats API for now and stores it into S3 bucket.
3. If it needs webscraping javascript enabled contents, it uses ph-webscrapper-lambda which uses Selenium. 


## Getting Started

### Prerequisites

- Python 3.8
- pip

### Installing

1. Clone the repository
2. Create a virtual environment: `python3 -m venv venv`
3. Activate the virtual environment: `source venv/bin/activate`
4. Install dependencies: `pip install -r requirements.txt`

### Running

1. Activate the virtual environment: `source venv/bin/activate`
2. Run the server: `python application.py`

## Technologies

- Flask