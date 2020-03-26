import os
import json


def main() -> None:
    with open("results.txt", "w") as f:
        f.write("")

    with open("partitioners.json", "r") as f:
        partitioners = json.loads(f.read())
        for partitioner in partitioners:
            os.system(
                f"""python3 ./mongodb_spark.py --PARTITIONER {partitioner["name"]} --CONFIG_NAME {partitioner["config"]["name"]} --CONFIG_VALUE {partitioner["config"]["value"]}"""
            )


if __name__ == "__main__":
    main()
