import dask.dataframe as dd
from datetime import datetime
import math
import dask.multiprocessing
from dask.distributed import Client, LocalCluster 
import time

def function_to_apply_row(x, y):
    return x*y

def benchmark_read(runs:int, file_name:str):
    print(__name__)
    times = []
    for i in range(0, runs):
        start = time.time()
        df = dd.read_csv(file_name,header=0, sep=',')
        print(df.shape[0].compute())
        t = time.time() - start
        times.append(t) 
    del df
    return str(sum(times)/len(times))

def benchmark_write(runs:int, file_name:str):
    print(__name__)
    times = []
    df = dd.read_csv(file_name,header=0, sep=',')
    for i in range(runs):
        start = time.time()
        df.to_csv('./output/output.csv', single_file=True, mode="w+")
        t = time.time() - start
        times.append(t) 
    del df
    return str(sum(times)/len(times))

def col_calc(runs:int, file_name:str):
    print(__name__)
    times = []
    for i in range(runs):
        df = dd.read_csv(file_name,header=0, sep=',')
        start = time.time()
        df["cost"]=df["Resource Unit Price"]*df["Resource Quantity"]
        print(df.shape[0].compute())
        t = time.time() - start
        times.append(t) 
    del df
    return str(sum(times)/len(times))

def apply_func(runs:int, file_name:str):
    print(__name__)
    times = []
    for i in range(runs):
        df = dd.read_csv(file_name,header=0, sep=',')
        start = time.time()
        df["cost"] = df.apply(lambda x : x["Resource Unit Price"]*x["Resource Quantity"], axis=1, meta=("float64"))
        print(df.shape[0].compute())
        t = time.time() - start
        times.append(t) 
    del df
    return str(sum(times)/len(times))

def apply_func_vect(runs:int, file_name:str):
    print(__name__)
    times = []
    for i in range(runs):
        df = dd.read_csv(file_name,header=0, sep=',')
        start = time.time()
        df["cost"] =function_to_apply_row(df["Resource Unit Price"],df["Resource Quantity"])
        print(df.shape[0].compute())
        t = time.time() - start
        times.append(t) 
    del df
    return str(sum(times)/len(times))

def group_by(runs:int, file_name:str):
    print(__name__)
    times = []
    for i in range(runs):
        df = dd.read_csv(file_name,header=0, sep=',')
        start = time.time()
        agg_df = df.groupby("Resource Vendor Name").agg({"Resource Quantity" : 'mean'})
        print(agg_df.shape[0].compute())
        t = time.time() - start
        times.append(t) 
        del agg_df
    del df
    return str(sum(times)/len(times))

def fill_na(runs:int, file_name:str):
    print(__name__)
    times = []
    for i in range(runs):
        df = dd.read_csv(file_name,header=0, sep=',')
        start = time.time()
        agg_df = df.fillna('0')
        print(agg_df.shape[0].compute())
        t = time.time() - start
        times.append(t) 
        del agg_df
    del df
    return str(sum(times)/len(times))

def main(type_processing):
    
    file_name = ['./data/Resources.csv', './data/resources_v2.csv']
    num = 10
    t1 = []
    t2 = []
    t3 = []
    t4 = []
    t5 = []
    t6 = []
    t7 = []
    for f in file_name:
        t1.append(benchmark_read(file_name=f, runs=num))
        t2.append(benchmark_write(file_name=f, runs=num))
        t3.append(col_calc(file_name=f, runs=num))
        t4.append(apply_func(file_name=f, runs=num))
        t5.append(group_by(file_name=f, runs=num))
        t6.append(fill_na(file_name=f, runs=num))
        t7.append(apply_func_vect(file_name=f, runs=num))
    with open('dask_results.txt', 'a') as f:
        for i in range(len(t1)):
            f.write(type_processing)
            f.write("\n")
            f.write(f"""
                File_name: {file_name[i]}
                Read: {t1[i]} 
                write: {t2[i]}
                Col with calc: {t3[i]}
                Col with func: {t4[i]}
                Col vector func: {t7[i]}
                group by: {t5[i]}
                fill_na: {t6[i]}
            """)
            f.write("\n")
if __name__ == '__main__':
    
    """
    client = Client(threads_per_worker=32, memory_limit='16GB', 
                    processes=False,
                    silence_logs='error')
    """
    types = ["synchronous", "threads", "processes"]
    for t in types:
        with dask.config.set(scheduler=t):
            main(t)
