import pandas as pd
from typing import Optional
from src.schema import NYC311Record
from src.reader import ReaderBase
from src.writer import WriterBase

class PipelineRunner:
    """
    This is my main ochestrator for the ETL. 
        Reader -> Schema Validation -> Writer
    I am intentionally decoupling from concrete implementations: 
        - reader must expose 'fetch' method
        - writer must implement writerbase interface (i.e. the write(df) method).
    Then the runner can be easily tests and extended. 
    """
    def __init__(self, reader: ReaderBase, writer: WriterBase):
        self.reader = reader
        self.writer = writer

    def run(self, **run_kwargs) -> None:
        """
        First implementation implements a single ETL Cycle: fetch -> clean -> wrote.
        """
        print(f"PipelineRunner: Fetching raw data with raw run params: {run_kwargs}")
        raw_records = self.reader.fetch(**run_kwargs)

        print(f"Pipeline Runner: Fetched {len(raw_records)} raw records.")

        cleaned = []
        print("PipelineRunner: Validating  + normalising records..")
        for rec in raw_records:
            try: 
                row = NYC311Record.from_api(rec)
                cleaned.append(row.to_dict())
            except ValueError:
                continue
        
        df = pd.DataFrame(cleaned)

        print(f"PipelineRunner: Passing {len(df)} cleaned records to writer...")
        self.writer.write(df)
        
        print(f"PipelineRunner: ETL Cycle complete.")
