from src.reader import NYC311Reader

def main():
    reader = NYC311Reader(limit=20)
    data = reader.fetch()
    print(f"Fetched {len(data)} records")
    print(data[:2])

if __name__ == "__main__":
    main()