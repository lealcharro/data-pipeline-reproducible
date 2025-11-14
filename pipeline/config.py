from dotenv import load_dotenv
import os

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", default="INFO")
INPUT_DIR = os.getenv("INPUT_DIR", default="/data/input")
INTERMEDIATE_DIR = os.getenv("INTERMEDIATE_DIR", default="/data/intermediate")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", default="/data/output")
