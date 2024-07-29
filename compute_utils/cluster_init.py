from dask.distributed import  LocalCluster

def create_local_dask_cluster(n_workers=2, threads_per_worker=1):
    """
    Create a local Dask cluster.
    
    Args:
        n_workers: Number of workers. If None, uses number of cores.
        threads_per_worker: Threads per worker. If None, uses 1.
    
    Returns:
        Dask Cluster object connected of the local cluster.
    """
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=threads_per_worker)
    return cluster