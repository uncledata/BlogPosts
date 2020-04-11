import pandas as pd
import random
import os


configurations = [
    {"row_count": 10000, "file_path": "./data/csv/KB"},
    {"row_count": 100000, "file_path": "./data/csv/1MB"},
    {"row_count": 1000000, "file_path": "./data/csv/11MB"},
]
for conf in configurations:
    for i in range(1000, 2000):
        pd.DataFrame(
            [
                [x % 1000000, random.randint(1, 900)]
                for x in range(0, conf["row_count"])
            ],
            columns=["key", "value"],
        ).to_csv(f"""{conf["file_path"]}/sample_file_{i}.csv""", index=False)
