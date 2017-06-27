#!/bin/bash
#
# Copyright (c) 2011, 2014-2017, Yong Qin <yong.qin@lbl.gov>. All rights reserved.
#
# This script demonstrates how to achieve high throughput by running 
# multiple tasks (MPMD or SPMD) within a given PBS or SLURM allocation.
#
# Version: 2.0 (May 17, 2017)
#


# GLOBAL settings
# SCHEDULER is one of [PBS, SLURM, MPI]
SCHEDULER=""
# LAUNCHER is "mpirun" for PBS or MPI, "srun" for SLURM
LAUNCHER=""
# Extra launcher options
LAUNCHER_OPT=""
MPI=""
HOSTFILE=""
TASKFILE=""
WORKDIR=""
HOSTFILE_PREFIX=""
TASKFILE_PREFIX=""
LOGFILE_PREFIX=""
# Extra env modules to load
MODULES=""
VERSION="2.0 (May 17, 2017)"

# IDs of all tasks (indexed array)
declare -a TASKS_ID
# All tasks (associative array)
declare -A TASKS
# Exit status of all tasks (associative array)
declare -A TASKS_EXIT
# IDs of running tasks
declare -a RUN_ID
# PIDs of running tasks
declare -a RUN_PID

# # of times to repeat taskfile (to generate duplicate tasks)
N_REPEAT=1

# Total # of tasks
N_TASKS=0
# Total # of succeeded tasks
N_SUCCESS=0
# Total # of slots available
N_SLOTS=0
# # of slots for each task
N_TASKSLOTS=0
# # of tasks to be run in parallel
N_TASKINPAR=0
# sweep schedule (seconds)
SLEEPTIME=60
LOGFILE=0
VERBOSE=0
DEBUG=0


# GLOBAL functions
function Error () {
    local EXIT="$1"
    local MSG="$2"
    echo "`date +"%b %d %H:%M:%S"` ERROR: "$MSG"" >&2
    exit $EXIT
}


function Warning () {
    local MSG="$1"
    echo "`date +"%b %d %H:%M:%S"` WARNING: "$MSG"" >&2
}


function Info () {
    local MSG="$1"
    if [[ $VERBOSE -eq 1 ]]; then
        echo "`date +"%b %d %H:%M:%S"` INFO: "$MSG"" >&2
    fi
}


function Debug () {
    local MSG="$1"
    if [[ $DEBUG -eq 1 ]]; then
        echo "`date +"%b %d %H:%M:%S"` DEBUG: "$MSG"" >&2
    fi
}


# Check if input value is a number or not.
# $1 is the input value
function IsNumber () {
    local INPUT=$1
    expr $INPUT + 1 >/dev/null 2>&1
    return $?
}


# Convert a range string to a list of numbers (array), e.g., 1-3,4,8-9->(1 2 3 4 8 9)
# $1 is the string to be processed
# RETURNVAL is the array holding the values
function NumRange () {
    local str1=$1
    # Strip out all white spaces
    local str2=${str1//[[:space:]]}
    # Separate ',' separated items to array elements
    local arr1=(${str2//,/ })
    local arr2=( )
    local arr3=( )
    local sec start end
    RETURNVAL=( )

    for sec in ${arr1[*]}; do
        # For each item, replace '-' with space and put them into an array
        arr2=(${sec//-/ })

        # If two items process them as start and end and expand
        if [[ ${#arr2[*]} -eq 2 ]]; then
            start=${arr2[0]}
            end=${arr2[1]}

            if ! IsNumber $start || ! IsNumber $end; then
                Error 1 "Invalid number range ${str1}!"
            fi

            if [[ $start -gt $end ]]; then
                read start end <<< "$end $start"
            fi
            for ((i = $start; i <= $end; i++)); do
                arr3+=($i)
            done
        elif [[ ${#arr2[*]} -eq 1 ]]; then
            if ! IsNumber ${arr2[0]}; then
                Error 1 "Invalid number range ${str1}!"
            fi

            arr3+=(${arr2[0]})
        else
            Error 1 "Invalid number range ${str1}!"
        fi
    done

    RETURNVAL=($(echo "${arr3[*]}" | sed 's/ /\n/g' | sort -nu))
}


# Load a file into an array, one per line. Empty or comment lines will be ignored.
# $1 is the file to be loaded
# RETUNVAL is the array holding the lines
function LoadFile () {
    local FILE="$1"
    local IFS=$'\n'
    local LINE
    RETURNVAL=( )

    while read LINE ; do
        # Strip whitespaces.
        while [ "${LINE## }" != "${LINE%% }" ]; do
            LINE="${LINE## }"
            LINE="${LINE%% }"
        done

        # Skip comments.
        if [ "${LINE###}" != "$LINE" ]; then
            continue
        fi

        # Skip empty lines.
        if [ "$LINE" == "" ]; then
            continue
        fi

        # Add the line to the list.
        RETURNVAL[${#RETURNVAL[*]}]="$LINE"
    done < "$FILE"
}


# Initialize the HTC mini scheduler.
function Init () {
    if [[ -z ${WORKDIR} ]]; then
        WORKDIR=${PWD}
    fi

    if [[ -n ${SLURM_JOB_ID} ]]; then
        SCHEDULER="SLURM"
        if [[ -z ${LAUNCHER} ]]; then
            LAUNCHER="mpirun"
        fi

        HOSTFILE_PREFIX=${WORKDIR}/${SLURM_JOB_NAME##*/}.${SLURM_JOB_ID}.host
        TASKFILE_PREFIX=${WORKDIR}/${SLURM_JOB_NAME##*/}.${SLURM_JOB_ID}.task
        LOGFILE_PREFIX=${WORKDIR}/${SLURM_JOB_NAME##*/}.${SLURM_JOB_ID}.log

        HOSTFILE=${HOSTFILE_PREFIX}
        srun /bin/hostname | sort > ${HOSTFILE}

        if [[ $? -ne 0 ]]; then
            Error 1 "Error on generating HOSTFILE!"
        fi
    elif [[ -n ${PBS_JOBID} ]]; then
        SCHEDULER="PBS"
        if [[ -z ${LAUNCHER} ]]; then
            LAUNCHER="mpirun"
        fi

        HOSTFILE_PREFIX=${WORKDIR}/${PBS_JOBNAME}.${PBS_JOBID}.host
        TASKFILE_PREFIX=${WORKDIR}/${PBS_JOBNAME}.${PBS_JOBID}.task
        LOGFILE_PREFIX=${WORKDIR}/${PBS_JOBNAME}.${PBS_JOBID}.log

        HOSTFILE=${HOSTFILE_PREFIX}
        cat $PBS_NODEFILE | sort > ${HOSTFILE}

        if [[ $? -ne 0 ]]; then
            Error 1 "Error on generating HOSTFILE!"
        fi
    else
        SCHEDULER="MPI"
        if [[ -z ${LAUNCHER} ]]; then
            LAUNCHER="mpirun"
        fi

        HOSTFILE_PREFIX=${WORKDIR}/$$.host
        TASKFILE_PREFIX=${WORKDIR}/$$.task
        LOGFILE_PREFIX=${WORKDIR}/$$.log

        if [[ -z ${HOSTFILE} ]]; then
            Error 1 "HOSTFILE is not provided and a scheduler allocation is not detected!"
        fi
    fi

    Info "Scheduler ${SCHEDULER} detected"
    Info "Launcher ${LAUNCHER} used"

    if [[ ${LAUNCHER} == "srun" ]]; then
        if [[ ${LAUNCHER_OPT} != *"--mem"* ]]; then
            LAUNCHER_OPT="--mem=1g ${LAUNCHER_OPT}"
            Info "Default Launcher option \"--mem=2g\" (per task) is used, please use \"-o\" to change if needed"
        fi
    fi

    if [[ ${LAUNCHER} == "mpirun" ]]; then
        if [[ ${SCHEDULER} == "SLURM" ]]; then
            unset SLURM_JOBID
        fi
        PrepMPI
    fi

    # Check hostfile.
    if [[ ! -r ${HOSTFILE} ]]; then
        Error 1 "${HOSTFILE} is not accessible!"
    fi

    # Check taskfile.
    if [[ -z ${TASKFILE} ]]; then
        Error 1 "${TASKFILE} is not provided!"
    fi

    if [[ ! -r ${TASKFILE} ]]; then
        Error 1 "${TASKFILE} is not accessible!"
    fi

    # Load taskfile.
    Info "Processing ${TASKFILE}"
    LoadFile "${TASKFILE}"
    Info "${TASKFILE} processed"

    local n=${#RETURNVAL[*]}
    Debug "$n tasks loaded from ${TASKFILE}"

    # If list of TASKS_ID not provided, initialize it.
    if [[ ${#TASKS_ID[*]} -eq 0 ]]; then
        for i in `seq 0 $((${N_REPEAT} * $n - 1))`; do
            TASKS_ID[$i]=$i
        done
    fi

    Debug "${#TASKS_ID[*]} task IDs initialized"

    # Populate tasks.
    for i in ${TASKS_ID[*]}; do
        TASKS[$i]=${RETURNVAL[((i % n))]}
    done

    Debug "${#TASKS[*]} tasks initialized"

    N_TASKS=${#TASKS[*]}
    if [[ ${N_TASKS} -lt 1 ]]; then
        Error 1 "Empty task list!"
    fi

    Info "${N_TASKS} tasks loaded"

    # Generate fine-grained allocation.
    N_SLOTS=`awk 'END{print NR}' ${HOSTFILE}`

    Info "${N_SLOTS} slots detected"

    if [[ ${N_SLOTS} -lt 1 ]]; then
        Error 1 "Total number of slots is less than 1!"
    fi
    if [[ ${N_TASKSLOTS} -gt ${N_SLOTS} ]]; then
        Warning "Number of task slots is greater than number of available slots, lowering it to the number of available slots ..."
        N_TASKSLOTS=${N_SLOTS}
    fi
    if [[ ${N_TASKSLOTS} -gt 0 && ${N_TASKINPAR} -gt 0 ]]; then
        Warning "-n and -p are provided at the same time, force to use -p ..."
        N_TASKSLOTS=$((${N_SLOTS} / ${N_TASKINPAR}))
    elif [[ ${N_TASKSLOTS} -gt 0 ]]; then
        N_TASKINPAR=$((${N_SLOTS} / ${N_TASKSLOTS}))
    elif [[ ${N_TASKINPAR} -gt 0 ]]; then
        N_TASKSLOTS=$((${N_SLOTS} / ${N_TASKINPAR}))
    else
        N_TASKSLOTS=1
        N_TASKINPAR=${N_SLOTS}
    fi
    if [[ $((${N_TASKINPAR} * ${N_TASKSLOTS})) -ne ${N_SLOTS} ]]; then
        Warning "Available slots (${N_SLOTS}) does not match task requirement (${N_TASKINPAR}x${N_TASKSLOTS}) ..."
    fi
    if [[ ${N_TASKINPAR} -gt ${N_TASKS} ]]; then
        Warning "Only ${N_TASKS} tasks are provided, lowering number of parallel tasks to ${N_TASKS} ..."
        N_TASKINPAR=${N_TASKS}
    fi

    Info "${N_TASKINPAR} tasks will be run in parallel"
}


# Build a task - takes one argument.
# $1 is current task id (HT_TASK_ID)
function BuildTask () {
    local HT_TASK_ID=$1
    cat > "${TASKFILE_PREFIX}.${HT_TASK_ID}" << EOF
#!${SHELL}
${TASKS[${HT_TASK_ID}]}
EOF
    chmod +x "${TASKFILE_PREFIX}.${HT_TASK_ID}"
}


# Run the HTC mini scheduler.
function Run () {
    # # of digits for the hostfile suffix
    local HOSTFILE_SUFFIX_LEN=${#N_TASKINPAR}
    # # of running tasks
    local N_RUN=0
    # # of started tasks
    local N_START=0
    # ID of current task
    local HT_TASK_ID=0
    # PID of current task
    local HT_TASK_PID=0
    # Exit Status of current task
    local HT_TASK_STATUS=""
    # Slot ID 
    local SLOT_ID=""

    # Prepare hostfiles for tasks.
    split -d -a ${HOSTFILE_SUFFIX_LEN} -l ${N_TASKSLOTS} "${HOSTFILE}" "${HOSTFILE_PREFIX}."

    # Initialize task ID & PID list.
    for i in `seq 0 $((${N_TASKINPAR} - 1))`; do
        RUN_ID[$i]=-1
        RUN_PID[$i]=-1
    done
    
    # Load necessary modules.
    if [[ -n ${MODULES} ]]; then
        module load "${MODULES}"
    fi

    # Mini scheduler.
    while true; do
        for i in `seq 0 $((${N_TASKINPAR} - 1))`; do
            SLOT_ID=`printf "%0${HOSTFILE_SUFFIX_LEN}d" $i`
    
            HT_TASK_ID=${RUN_ID[$i]}
            HT_TASK_PID=${RUN_PID[$i]}

            # Check to see if the task is still running or not.
            kill -0 ${HT_TASK_PID} >/dev/null 2>&1
    
            if [[ $? -ne 0 || ${HT_TASK_PID} -eq -1 ]]; then
                if [[ ${HT_TASK_PID} -ne -1 ]]; then
                    # Retrieve task exit status.
                    wait ${HT_TASK_PID} >/dev/null 2>&1
                    TASKS_EXIT[${HT_TASK_ID}]=$?
                    if [[ ${TASKS_EXIT[${HT_TASK_ID}]} -eq 0 ]]; then
                        HT_TASK_STATUS="succeeded"
                        N_SUCCESS=$((${N_SUCCESS} + 1))
                    else
                        HT_TASK_STATUS="failed (exit code: ${TASKS_EXIT[${HT_TASK_ID}]})"
                    fi

                    local MSG="Task ${HT_TASK_ID} ${HT_TASK_STATUS}"
                    if [[ ${DEBUG} -eq 1 ]]; then
                        MSG+=" from ${TASKFILE_PREFIX}.${HT_TASK_ID} with ${HOSTFILE_PREFIX}.${SLOT_ID}"
                    fi
                    if [[ ${LOGFILE} -eq 1 ]]; then
                        MSG+=" logged to ${LOGFILE_PREFIX}.${HT_TASK_ID}"
                    fi
                    Info "$MSG"

                    RUN_ID[$i]=-1
                    RUN_PID[$i]=-1
                    N_RUN=$((${N_RUN} - 1))
                fi
    
                HT_TASK_ID=${TASKS_ID[${N_START}]}
                if [[ ${N_START} -lt ${N_TASKS} ]]; then
                    # Compose and run a new task, store its PID in ${RUN_PID[$i]}.
                    BuildTask "${HT_TASK_ID}"
    
                    local LAUNCHER_EXT="${LAUNCHER_OPT}"
                    if [[ ${LAUNCHER} == "srun" ]]; then
                        LAUNCHER_EXT+=" -n ${N_TASKSLOTS}"
                        LAUNCHER_EXT+=" -N `awk 'END{print NR}' ${HOSTFILE_PREFIX}.${SLOT_ID}`"
                        LAUNCHER_EXT+=" --export=ALL,HT_TASK_ID=${HT_TASK_ID}"
                    elif [[ ${LAUNCHER} == "mpirun" ]]; then
                        LAUNCHER_EXT+=" -hostfile ${HOSTFILE_PREFIX}.${SLOT_ID}"
                        LAUNCHER_EXT+=" -n ${N_TASKSLOTS}"
                        LAUNCHER_EXT+=" -x HT_TASK_ID=${HT_TASK_ID}"
                    fi
    
                    if [[ ${LAUNCHER} == "srun" ]]; then
                        if [[ ${LOGFILE} -eq 1 ]]; then
                            Debug "Task ${HT_TASK_ID}: SLURM_HOSTFILE=${HOSTFILE_PREFIX}.${SLOT_ID} ${LAUNCHER} ${LAUNCHER_EXT} ${TASKFILE_PREFIX}.${HT_TASK_ID} > ${LOGFILE_PREFIX}.${HT_TASK_ID}"
                            SLURM_HOSTFILE=${HOSTFILE_PREFIX}.${SLOT_ID} ${LAUNCHER} ${LAUNCHER_EXT} ${TASKFILE_PREFIX}.${HT_TASK_ID} > ${LOGFILE_PREFIX}.${HT_TASK_ID} 2>&1 &
                        else
                            Debug "Task ${HT_TASK_ID}: SLURM_HOSTFILE=${HOSTFILE_PREFIX}.${SLOT_ID} ${LAUNCHER} ${LAUNCHER_EXT} ${TASKFILE_PREFIX}.${HT_TASK_ID}"
                            SLURM_HOSTFILE=${HOSTFILE_PREFIX}.${SLOT_ID} ${LAUNCHER} ${LAUNCHER_EXT} ${TASKFILE_PREFIX}.${HT_TASK_ID} &
                        fi
                    elif [[ ${LAUNCHER} == "mpirun" ]]; then
                        if [[ ${LOGFILE} -eq 1 ]]; then
                            Debug "Task ${HT_TASK_ID}: ${LAUNCHER} ${LAUNCHER_EXT} ${TASKFILE_PREFIX}.${HT_TASK_ID} > ${LOGFILE_PREFIX}.${HT_TASK_ID}"
                            ${LAUNCHER} ${LAUNCHER_EXT} ${TASKFILE_PREFIX}.${HT_TASK_ID} > ${LOGFILE_PREFIX}.${HT_TASK_ID} 2>&1 &
                        else
                            Debug "Task ${HT_TASK_ID}: ${LAUNCHER} ${LAUNCHER_EXT} ${TASKFILE_PREFIX}.${HT_TASK_ID}"
                            ${LAUNCHER} ${LAUNCHER_EXT} ${TASKFILE_PREFIX}.${HT_TASK_ID} &
                        fi
                    fi

                    HT_TASK_PID=$!

                    local MSG="Task ${HT_TASK_ID} started"
                    if [[ ${DEBUG} -eq 1 ]]; then
                        MSG+=" from ${TASKFILE_PREFIX}.${HT_TASK_ID} with ${HOSTFILE_PREFIX}.${SLOT_ID}"
                    fi
                    if [[ ${LOGFILE} -eq 1 ]]; then
                        MSG+=" logged to ${LOGFILE_PREFIX}.${HT_TASK_ID}"
                    fi
                    Info "$MSG"

                    RUN_ID[$i]=${HT_TASK_ID}
                    RUN_PID[$i]=${HT_TASK_PID}
                    N_START=$((${N_START} + 1))
                    N_RUN=$((${N_RUN} + 1))
                fi
            fi
        done
    
        if [[ ${N_START} -ge ${N_TASKS} && ${N_RUN} -eq 0 ]]; then
            break
        fi
    
        sleep ${SLEEPTIME}
    done
}


# Prepare the MPI environment.
function PrepMPI () {
    # TODO: Need to acommodate other MPIs as well
    MPI="OPENMPI"

    module -t list 2>&1 | grep ^openmpi >/dev/null
    if [[ $? -ne 0 ]]; then
        module load openmpi >/dev/null 2>&1
    fi
    MPIRUN=`which mpirun 2>/dev/null`
    if [[ $? -ne 0 ]]; then
        Error 1 "Cannot locate Open MPI!"
    fi
    OPENMPI_DIR=${OPENMPI_DIR:-$MPIDIR}
    if [[ -z $OPENMPI_DIR ]]; then
        Warning "Open MPI prefix not defined, try without ..."
    fi
    if [[ -n $OPENMPI_DIR ]]; then
        LAUNCHER_OPT="-prefix $OPENMPI_DIR ${LAUNCHER_OPT}"
    fi
}


# Clean up HT runtime environment.
function Clean () {
    if [[ ${DEBUG} -eq 0 ]]; then
        rm -f ${HOSTFILE_PREFIX} ${HOSTFILE_PREFIX}.* ${TASKFILE_PREFIX}.* >/dev/null 2>&1
    fi

    for i in `seq 0 $((${N_TASKINPAR} - 1))`; do
        if [[ ${RUN_PID[$i]} -ne -1 ]]; then
            kill -TERM -- -${RUN_PID[$i]} >/dev/null 2>&1
        fi
    done
}


function Usage () {
    echo "Usage: $0 [-hLv] [-f hostfile] [-i list] [-l launcher] [-m modules] [-n # of slots per task] [-o launcher options] [-p # of parallel tasks] [-r # of repeat] [-s sleep] [-t taskfile] [-w workdir]"
    echo "    -f    provide a hostfile with list of slots, one per line"
    echo "    -h    this help page"
    echo "    -i    provide list of tasks from the taskfile to run, e.g., 1-3,5,7-9"
    echo "    -l    override system launcher (mpirun only for now)"
    echo "    -L    log task stdout/stderr to individual files"
    echo "    -m    provide env modules to be loaded for tasks (comma separated)"
    echo "    -n    provide number of slots per task"
    echo "    -o    provide extra launcher options, e.g., \"-mca btl openib,sm,self\""
    echo "    -p    provide number of parallel tasks"
    echo "    -r    provide repeat number for taskfile"
    echo "    -s    interval between checks (default to 60s)"
    echo "    -t    provide a taskfile with list of tasks, one per line (required)"
    echo "          task could be a binary executable, or a script"
    echo "          multiple steps within the same task can be semicolon separated, but they have to remain on the same line"
    echo "          env variable HT_TASK_ID (starts from 0) can be used with individual tasks"
    echo "    -v    verbose mode"
    echo "    -w    provide work directory (default to current directory)"
}


# Retrieve command line options.
# TODO: -d and -k should be removed in future releases
while getopts ":dDf:hi:kl:Lm:n:o:p:r:s:t:vw:" OPT; do
    case $OPT in
        d)
            Warning "\"-d\" has been deprecated, please use \"-L\" instead"
            LOGFILE=1
            ;;
        D)
            DEBUG=1
            ;;
        f)
            if [[ -f ${OPTARG} ]]; then
                HOSTFILE=${OPTARG}
            else
                Error 1 "${OPTARG} does not exist!"
            fi
            ;;
        h)
            Usage
            exit 0
            ;;
        i)
            NumRange ${OPTARG}
            TASKS_ID=(${RETURNVAL[*]})
            ;;
        k)
            Warning "\"-k\" has been deprecated"
            ;;
        l)
            if [[ ${OPTARG} == "srun" || ${OPTARG} == "mpirun" ]]; then
                LAUNCHER=${OPTARG}
            else
                Error 1 "${OPTARG} is not a valid launcher!"
            fi
            ;;
        L)
            LOGFILE=1
            ;;
        m)
            MODULES=${OPTARG//,/ }
            ;;
        n)
            if IsNumber ${OPTARG} && [[ ${OPTARG} -gt 0 ]]; then
                N_TASKSLOTS=${OPTARG}
            else
                Error 1 "Invalid argument for -$OPT: ${OPTARG}!"
            fi
            ;;
        o)
            LAUNCHER_OPT=${OPTARG}
            ;;
        p)
            if IsNumber ${OPTARG}  && [[ ${OPTARG} -gt 0 ]]; then
                N_TASKINPAR=${OPTARG}
            else
                Error 1 "Invalid argument for -$OPT: ${OPTARG}!"
            fi
            ;;
        r)
            if IsNumber ${OPTARG}  && [[ ${OPTARG} -gt 0 ]]; then
                N_REPEAT=${OPTARG}
            else
                Error 1 "Invalid argument for -$OPT: ${OPTARG}!"
            fi
            ;;
        s)
            IsNumber ${OPTARG}
            if IsNumber ${OPTARG}  && [[ ${OPTARG} -ge 0 ]]; then
                SLEEPTIME=${OPTARG}
            else
                Error 1 "Invalid argument for -$OPT: ${OPTARG}!"
            fi
            ;;
        t)
            if [[ -f ${OPTARG} ]]; then
                TASKFILE=${OPTARG}
            else
                Error 1 "${OPTARG} does not exist!"
            fi
            ;;
        v)
            VERBOSE=1
            ;;
        w)
            if [[ -d ${OPTARG} ]]; then
                WORKDIR=${OPTARG}
            else
                Error 1 "${OPTARG} does not exist!"
            fi
            ;;
        \?)
            Error 1 "Invalid option: -${OPTARG}!"
            ;;
        :)
            Error 1 "Option -${OPTARG} requires an argument!"
            ;;
    esac
done

# Sanity check.
if [[ -z $1 ]]; then
    Usage
    exit 0
fi

trap "Clean" EXIT SIGINT SIGTERM

Info "HT Helper ${VERSION}"
Debug "$0 $*"

# Prepare the runtime environment.
Init

Info "Proceed with ${N_TASKINPAR} parallel task(s) and ${N_TASKSLOTS} slot(s) per task on ${N_TASKS} task(s) and a ${N_SLOTS}-slot allocation"

# Run tasks.
Run

Info "${N_SUCCESS} task(s) succeeded"
if [[ $((${N_TASKS}-${N_SUCCESS})) -gt 0 ]]; then
    Info "$((${N_TASKS}-${N_SUCCESS})) task(s) failed"
fi
