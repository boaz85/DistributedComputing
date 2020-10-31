# Distributed Computing

Distributed GPU computations made easy.

### Example

Define a worker class that implements the worker interface:

```
from distributed_computing.api import WorkerInterface

class WorkerClass(WorkerInterface):
    def __init__(self, init_data):
        # Heavy preparation work is done here (once for each worker)
        download_dataset()
        preprocess_dataset()
        do_heavy_work()
    
    def run(parameter_1, parameter_2, parameter_3=None):
        # The worker is done here
        train_network(parameter_1, parameter_2)
        result = evaluate_network(parameter_3)
        
        return result

    def handle_update(self, cache_update):
        # Handle updates sent by the pool owner
        update_cache(cache_update)
```

Start a head node on machine A:
```
>>> start_node --head --password=123456

******************************************************************************************
To start a worker node, type:
start_node -p 8899 -a 194.173.121.37 -r 123456
******************************************************************************************
```

A separate isolated instance of the worker class is initiated and assigned with each available GPU on machine A.
Start worker node on machine B:
```
start_node -p 8899 -a 194.173.121.37 -r 123456
```

A separate isolated instance of the worker class is initiated and assigned with each available GPU on machine B.
Now, you can connect to the head node and use your workers pool:
```
from distributed_computing.api import Pool

with Pool(WorkerClass, {'args': [init_data]}, '123456', '194.173.121.37') as p:
    results = p.map(parameter_sets)

```

Alternatively, if your workers can benefit from having the results already obtained by other workers, it is possible to
 update all workers with each result:

```
from distributed_computing.api import Pool

results = []

with Pool(WorkerClass, {'args': [init_data]}, '123456', '194.173.121.37') as p:
    for result in p.imap_unordered(parameter_sets):
        results.append(result)
        p.update_workers(result)
```

This way, your workers can maintain a shared cache, for example.

* Worker nodes can be started / stopped dynamically, also after the pool has started. Only the head node must keep running.  
* If one of your worker nodes dies while working, the jobs it was executing are automatically rescheduled and processed 
by other available workers.
* Multiple *Pool* objects may be created by multiple scripts, but not simultaneously.
* Only a single GPU is visible for each worker. The number of workers on each machine is determined by the number of
 available GPU devices. Initialization data, run parameters, updates data, results and worker classes must all be picklable.
* The jobs / results / update queues are limited (each) to 512Mb. Keep that in mind.
* Jobs and updates are handled synchronously. Updates are processed only between jobs.
* Only a single instance of a worker node can run on each machine. If there's a node already running, start_node will 
exit peacefully. 
* Nodes may be started using Python code (rather than the CLI command). See API reference.
* Redis port (6379) and the pool port (default: 8899) must be open in the head node.


### API reference


```
distributed_computing.api.start_redis(password)
```
Start a dedicated redis server on the local machine.  
  
**password** worker pool password to prevent unauthorized access.  
  
**returns** None if the redis server is already running, or a process handle if it has been started by this function.  


```
distributed_computing.api.start_node(password, address='localhost', port=DEFAULT_PORT, violent_exit=True)
```
Start a worker node on the local machine.  
  
**password** worker pool password to prevent unauthorized access.  
**address** workers pool server address. If not provided, assumed to be the local machine (localhost).  
**port** workers pool server port.  
**violent_exit** Use sys.exit(1) to kill the node when SIGKILL is triggered.  
  
**returns** a function that terminates the node process. A function that waits for it to finish.  

```
distributed_computing.api.start_server(password, port=DEFAULT_PORT)
```
Start the worker pool server on the local machine. May be used to separate pool management from worker machines.
A redis server is also started by this function if not already running.  
  
**password** worker pool password to prevent unauthorized access.  
**port** workers pool server port.  
  
**returns** a function that terminates the pool server process. A function that waits for it to finish.  

```
distributed_computing.api.start_head_node(password, port=DEFAULT_PORT)
```
Start a worker pool server and a worker node on the local machine. A redis server is also started by this function
if not already running.  
  
**password** worker pool password to prevent unauthorized access.  
**port** workers pool server port.  
  
**returns** a function that terminates both the server and the node process. A function that waits for both it to
finish.  

```   
distributed_computing.api.is_server_running()
```
Check if a worker pool server is already running on the local machine.  
  
**returns** True if a server is running, else False.  

```
distributed_computing.api.is_client_running()
```
Check if a worker node is already running on the local machine.  
  
**returns** True if a server is running, else False.  

```
distributed_computing.api.Pool(object)
```
Worker pool interface, similar to python multiprocessing Pool interface.  
  
```
distributed_computing.api.Pool.__init__(self, worker_class, init_data, password, server_address='localhost', server_port=DEFAULT_PORT)
```
Initialize pool and connect to the worker pool server.  
  
**worker_class** A worker class that implements the WorkerInterface. This class is instantiated on each
worker process in the worker machines.  
**init_data** Initialization data that is passed to the worker class constructor. This is a dictionary
of the form {'args': [...], 'kwargs': {...}}. The args are provided to the worker class constructor as args,
and the kwargs are provided as keyword args.  
**password** worker pool password to prevent unauthorized access.  
**server_address** workers pool server address. If not provided, assumed to be the local machine
(localhost).  
**server_port** workers pool server port.  

```
distributed_computing.api.Pool.imap_unordered(self, data)
```
Return results once ready. Order is not guaranteed. Similar to multiprocessing.Pool.imap_unordered.  
  
**data** List of items to be processed by the worker. This is a dictionary
of the form {'args': [...], 'kwargs': {...}}. The args are provided to the worker class run method as args,
and the kwargs are provided as keyword args.  

```
distributed_computing.api.Pool.map(self, data)
```
Return all the results, ordered by the corresponding inputs ordering. Similar to multiprocessing.Pool.map.  
  
**data** List of items to be processed by the worker. This is a dictionary
of the form {'args': [...], 'kwargs': {...}}. The args are provided to the worker class run method as args,
and the kwargs are provided as keyword args.  
  
**returns** results list.  

```
distributed_computing.api.Pool.get_workers_count(self)
```
Get the current number of worker processes. Not to be confused with the number of worker nodes (machines).  
    
**returns** number of worker processes.  

```
distributed_computing.api.Pool.get_nodes_count(self)
```
Get the current number of worker nodes. Not to be confused with the number of worker processes.  
  
**returns** number of worker nodes.  

```
distributed_computing.api.Pool.update_workers(self, data)
```
Send a synchronous update to all worker processes. The workers will get this update once they finish the
current job, or immediately if no job is being processed.  
  
**data** update data that is passed to the worker handle_update method. This is a dictionary
of the form {'args': [...], 'kwargs': {...}}. The args are provided to the worker class handle_update method as
args, and the kwargs are provided as keyword args.  
  
**returns** None  

```
distributed_computing.api.Pool.close(self)
```
Close the current pool. Active jobs, if any, are aborted.  
  
**returns** None  

```
distributed_computing.api.WorkerInterface(object)
```
This interface must be implemented by the worker class provided to the Pool object.  

```
distributed_computing.api.WorkerInterface.run(self, *args, **kwargs)
```
This method is where the main work is done. It is called for each job, with the corresponding args and kwargs
provided to the pool.map or pool.imap_unordered functions. The returned value is passed to the caller as is.  

```
distributed_computing.api.WorkerInterface.handle_update(self, *args, **kwargs)
```
This method is called when the pool.update_workers is called by the pool owner with the provided args and
kwargs. The returned value is ignored.  

