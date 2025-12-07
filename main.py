import pandas as pd 

from src.reader import NYC311Reader
from src.schema import RequestNYCRecords

def main():
    reader = NYC311Reader(limit=20)
    data = reader.fetch()
    print(f"Fetched {len(data)} records")

    cleaned_record = []
    for record in data:
        try: 
            obj = RequestNYCRecords(**record)
            cleaned_record.append(obj.to_dict())
        except ValueError:
            continue
    df = pd.DataFrame(cleaned_record)
    print(df.head())


if __name__ == "__main__":
    main()