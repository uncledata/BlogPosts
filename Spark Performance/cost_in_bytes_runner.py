import os
import json
import time


def main() -> None:

    configurations = [
        {"file_path": "./data/csv/KB/", "maxpartitionbytes": "134217728"},
        {"file_path": "./data/csv/KB/", "maxpartitionbytes": "268435456"},
        {"file_path": "./data/csv/KB/", "maxpartitionbytes": "528435456"},
        {"file_path": "./data/csv/KB/", "maxpartitionbytes": "1048435456"},
        {"file_path": "./data/csv/1MB/", "maxpartitionbytes": "134217728"},
        {"file_path": "./data/csv/1MB/", "maxpartitionbytes": "268435456"},
        {"file_path": "./data/csv/1MB/", "maxpartitionbytes": "528435456"},
        {"file_path": "./data/csv/1MB/", "maxpartitionbytes": "1048435456"},
        {"file_path": "./data/csv/11MB/", "maxpartitionbytes": "134217728"},
        {"file_path": "./data/csv/11MB/", "maxpartitionbytes": "268435456"},
        {"file_path": "./data/csv/11MB/", "maxpartitionbytes": "528435456"},
        {"file_path": "./data/csv/11MB/", "maxpartitionbytes": "1048435456"},
    ]

    open_cost = [
        "597152",
        "1097152",
        "2097152",
        "4194304",
        "8388608",
        "16777216",
        "33554432",
        "67108864",
        "134217728",
        "268435456",
    ]
    counter = 0
    with open("results.txt", "w+") as f:
        for cost_param in open_cost:
            for conf in configurations:
                counter += 1
                start_time = time.time()
                print(counter)
                os.system(
                    f"""python3 ./maxPartitionBytes.py --FILE_PATH {conf["file_path"]} --MAXPARTITIONBYTES {conf["maxpartitionbytes"]} --OPENCOSTINBYTES {cost_param}"""
                )
                vals = f"""{conf["file_path"]}|{conf["maxpartitionbytes"]}|{cost_param}|{time.time()-start_time}\n"""
                f.write(vals)


if __name__ == "__main__":
    main()
