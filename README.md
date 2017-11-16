This is a fault tolerant distributed task scheduler simulation.

## Design

#### Problem Statement
As desciprtion. Todo...

#### Fault Tolenrance Solution
A task is considered to be done only if the *worker* notify *master* (which then remove the task from *task pool*), and *master* send ping message (heartbeat) to each *workers* regularly. So the failure of *worker* will only cause restarting the task. On the other hand, *task pool* node also keep track of which task *workers* are working on, in a mongodb document or in-memory (if we can assume this node is unlikely to be down), updated by *master*. If *master* is down and restarted, it reads the status and send ping message to each *worker* to see their current status. In this situation, an unfinished task is considered to be failed if it is not being worked on, in case *worker* has also been restarted. So by persisting the *workers* status, the failure of *master* can be recovered.

#### Architecture
I chose HTTP to be the protocol for the communication among nodes for its reusability and expressiveness, and the interfaces are design in a RESTful manner. A Flask server is set up within each module to serve the HTTP requests. Following is the modules and their interfaces design.
* **TaskPool**: keeps tasks in mongodb and persists *workers* information. Interface are accessed by *master* only.

&ensp;&ensp;&ensp;`\getJob`: to get a task from top of the list

&ensp;&ensp;&ensp;`\delJob`: to mark a task is done and remove from task list

&ensp;&ensp;&ensp;`\putStatus`: update worker status

* **Master**: monitoring and tasks assigning.

&ensp;&ensp;&ensp;`\notify`: for *worker* to notify *master* when a task is done

* **Worker**: nodes that do the tasks, in this simulation sleeping for a certain time period.

&ensp;&ensp;&ensp;`\ping`: to determine if the node is down.

&ensp;&ensp;&ensp;`\do`:

## Usage Instructions
Nodes are run in docker containers. Todo...

## Running
results. Todo...
