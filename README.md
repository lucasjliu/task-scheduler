This is a fault tolerant distributed task scheduler simulation.

## Design

#### Problem Statement
Tasks are created and stored in a persistent task pool. A master gets tasks from it and assign to workers. Each time a task runs, the worker will just sleep for the given sleeptime seconds, after than finish. Each worker can work on one task at a time, and has no access to the task pool. For fault tolerance, a simulation result is assumed to be correct only if:
* All tasks eventually finish and are logged
* Killing the master does not affect running tasks
* Killing a worker will kill tasks running on it
* Killed tasks will be restarted from scratch
* Master is able to find out worker failures

#### Scheduling
For each task, keep waiting until one of the available workers is picked randomly (simple load balance) to serve it.

#### Fault Tolenrance
Worker failure is handled in two ways: master send heartbeat message to workers regularly so is able to know the failure; otherwise if the worker is up before next heartbeat, it will notify master itself. Worker can know previous unfinished tasks by always keeping on-going tasks in persistence. For either handling the failed tasks are put back to the task pool.

For master failure, when master is restarted previous tasks will be either reported by the worker or considered failed if the worker is also down. Here a complicated situation may happen: master is killed, task-0 is done, worker A fails to notify master, and later A is also killed, then master is up and restarts task-0 (master does not wait for A because A could be down forever). After this, if A is up it should send the task-0 success message to master. To distinguish these two task-0, `current_tasks` is maintained in persistence by master in order to accept the on-going task-0 only.

#### Architecture
Data persistence is handled by MongoDB. HTTP is chosen to be the protocol for the communication among nodes, and the interfaces are design in a RESTful manner. A Flask server is set up within each module to serve the HTTP requests. Following is the modules and their interfaces design.
* **TaskPool**: keeps tasks in MongoDB, accessed by master.

&ensp;&ensp;&ensp;`/get`: to get a task from the list

&ensp;&ensp;&ensp;`/put`: to put back a task if it was failed

* **Master**: for monitoring and tasks assigning.

&ensp;&ensp;&ensp;`/notify`: for worker to notify master a success/failed task

* **Worker**: nodes that do the tasks.

&ensp;&ensp;&ensp;`/ping`: to determine if the node is down, and let the node notify master any previous task status. Note that this interface does not return the status to avoid race condition with `\notify`.

&ensp;&ensp;&ensp;`/doTask`: assign a task by master

## Documentation
```
./docker
    dockerfile for the image
./logs
    master: log file of master.py
./scheduler
    common.py: common utilities
    master.py: entry point for master node
    taskpool.py: entry point for taskpool node
    worker.py: entry point for worker node
./tests
    echo.py: a simple http application to verify essential libraries are installed
```

## Usage Instructions
Nodes are run in docker containers. All following command are peformed in project directory.

1. Use ```setup_nodes.sh``` to build the docker image and start containers for: a master, a taskpool, three workers
2. Run mongodb service, set /etc/hosts according to the comment in ```setup_nodes.sh```
3. (Optionl) Try ```./tests/echo.py```, no error message should appear
4. Start the taskpool: ```./scheduler/taskpool.py```
5. Start the master: ```./scheduler/master.py```
6. Start the workers with hostname as the parameter:
```
./scheduler/worker.py worker1
./scheduler/worker.py worker2
./scheduler/worker.py worker3
```
7. Kill master or workers with Ctrl-C and restart it. The failed tasks should appear in the bottom of the log file.

## Sample Result
```log_simple_10_tasks``` is a short piece of example running 10 tasks. Two parallel workers were scheduled correctly. And the failed task-5 due to worker1 was handled.

```log_3_workers_100_tasks``` is an example of 100 tasks with 3 workers. Here is what happened at running:
1. All workers were killed around line 15, and restarted one by one. Failed tasks 9, 11, 12 were handled (and appeared at the end of log file).
2. At line 40 master was killed and restarted and no error happened.
3. Master was killed again around line 55 with all workers also killed afterwards. However, worker1 was restarted before master is up, worker2 after master is up, and worker3 was never restarted. Worker1 did not issue a failure since it recovered by itself. Either failures issued by worker2 and 3 is properly handled.
4. At line 88 worker2 was killed before master was killed, the failure captured.
