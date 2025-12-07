import pandas as pd 

from src.reader import NYC311Reader
from src.runner import PipelineRunner
from src.writer import SQLiteWriter

def main():
    reader = NYC311Reader(limit=20)
    writer = SQLiteWriter()

    runner = PipelineRunner(reader, writer)
    runner.run() 

if __name__ == "__main__":
    main()