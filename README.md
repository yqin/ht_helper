# High-Throughput Computing (HTC) with HT Helper
If you have a set of common tasks that you would like to perform on the cluster,
and these tasks share the common characteristics of short duration and you have
a decent number of them, they fall into the category of High-Throughput
Computing (HTC). Typical applications such as parameter/configuration scanning,
divide and conquer approach can all be categorized in this section. Resolving an
HTC problem isn't easy on a traditional High-Performance Computing (HPC) cluster
with time and resource constraints. However, within the rooms that one can
maneuver, there are still some choices. HT Helper is designed to satisfy the
exact requirement by running HTC workloads within an HPC environment. The idea
of the HT Helper is to fire a FIFO mini scheduler within a real scheduler
allocation (SLURM, PBS, or a hostfile), then cycle through all the tasks within
the allocation by using the mini scheduler. These tasks could be either serial
or parallel.

# Usage
```
[joe@n0000 ]$ ht_helper.sh -h
Usage: ./ht_helper.sh [-dhLv] [-f hostfile] [-i list] [-l launcher] [-m modules] [-n # of slots per task] [-o launcher options] [-p # of parallel tasks] [-r # of repeat] [-s sleep] [-t taskfile] [-w wordir]
    -f    provide a hostfile with list of slots, one per line
    -h    this help page
    -i    provide list of tasks from the taskfile to run, e.g., 1-3,5,7-9
    -l    override system launcher (mpirun only for now)
    -L    log task stdout/stderr to individual files
    -m    provide env modules to be loaded for tasks (comma separated)
    -n    provide number of slots per task
    -o    provide extra launcher options, e.g., "-mca btl openib,sm,self"
    -p    provide number of parallel tasks
    -r    provide repeat number for taskfile
    -s    interval between checks (default to 60s)
    -t    provide a taskfile with list of tasks, one per line (required)
          task could be a binary executable, or a script
          multiple steps within the same task can be semicolon separated, but they have to remain on the same line
          env variable HT_TASK_ID (starts from 0) can be used with individual tasks
    -v    verbose mode
    -w    provide work directory (default to current directory)
```

To use the helper script you will need to prepare a taskfile and a job script
file. The taskfile will contain all the tasks that you need to run. If a self
identifier is desired for each task, environment variable *$HT_TASK_ID* can be
used in the taskfile, or any of the subsequent scripts. The taskfile takes three
types of input as showed in the usage page. If you are running MPI type of
tasks, please make sure not to have the mpirun command in the taskfile, instead
you only need to input the actual executable and input options. If mpirun
command line options are required please provide them via the "-o" option. For
users running parallel tasks, please make sure to turn off CPU affinity
settings, if any, to avoid conflicts and serious oversubscription of CPUs. The
next important parameter is the "-n" option - how many processors/cpus you want
to allocate for each task, the default value is "1" for serial tasks if not
provided. If you are running short-duration tasks (less than a few minutes), you
may also want to reduce the default mini scheduler scan interval from 60 seconds
to a smaller value with the "-s" option. If you are running within an SLURM or
PBS allocation, please do not specify the hostfile with "-f" option which may
conflict with the default allocation. To get familiar with using this helper
script, you may want to turn on "-L" (log output from each task to an individual
file), and "-v" (verbose mode) options so that you can better understand how it
works. After you are familiar with the process, you can choose which options to
use, we recommend "-L" and "-v". For the job script file it will look similar to
a job script for a parallel job, except that you want to run command
"ht_helper.sh" on the taskfile that was just prepared instead of anything else.

# Example
## Running 8 serial tasks within a 4-CPU allocation

    taskfile:
```
    hostname
    date
    ls
    whoami
    uname -a
    pwd
    echo task $HT_TASK_ID; hostname; sleep 5
    echo $((1+1))
```

    SLURM job script:
```
    #!/bin/bash
    #SBATCH --job-name=test
    #SBATCH --partition=test
    #SBATCH --qos=debug
    #SBATCH --account=abc
    #SBATCH --ntasks=4
    #SBATCH --time=00:05:00

    ht_helper.sh -t taskfile -n1 -s1 -vL
```

## Running 1000 OMP tasks with 2 threads per task on 8 12-core nodes

    taskfile:
```
    a.out $HT_TASK_ID
```

    SLURM job script:
```
    #!/bin/bash
    #SBATCH --job-name=test
    #SBATCH --partition=test
    #SBATCH --qos=debug
    #SBATCH --account=abc
    #SBATCH --nodes=8
    #SBATCH --ntasks-per-node=6
    #SBATCH --cpus-per-task=2
    #SBATCH --time=00:05:00

    export OMP_NUM_THREADS=2
    ht_helper.sh -t taskfile -r1000 -n1 -vL
```

## Running MPI tasks 1-3, 7, 9-15 with 4 processors per task on 2 12-core nodes

    taskfile
```
    a.out $HT_TASK_ID
```

    SLURM job script:
```
    #!/bin/bash
    #SBATCH --job-name=test
    #SBATCH --partition=test
    #SBATCH --qos=debug
    #SBATCH --account=abc
    #SBATCH --nodes=2
    #SBATCH --ntasks-per-node=12
    #SBATCH --time=00:05:00

    ht_helper.sh -t taskfile -i 1-3,7,9-15 -n4 -vL
```
