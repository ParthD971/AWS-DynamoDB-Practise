import os

from dotenv import load_dotenv
load_dotenv()

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_RESOURCE_NAME = os.environ.get('AWS_RESOURCE_NAME')
AWS_REGION_NAME = os.environ.get('AWS_REGION_NAME')
TABLE_NAME = os.environ.get('TABLE_NAME')
