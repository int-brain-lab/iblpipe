"""IBL pipeline.

A task type is defined by:

- a name (also name of a Python function)
- a list of relative paths (possibly with a *) to input files/directories (relative to the
  session directory)
- a list of relative paths (possibly with a *) to output files/directories (relative to the
  session directory)
- a list of task type that should follow the current task type (dependencies)
- a default n_cpus
- a default n_gpus
- a default io_charge (integer between 0 and 100)
- a default priority

A task request is defined by:

- the task type's name
- a list of absolute full input paths
- a list of absolute full output paths
- an optional overriding n_cpus
- an optional overriding n_gpus
- an optional overriding io_charge
- an optional overriding priority

A task run is defined by:

- a task request
- a current status
- a started datetime
- a completed datetime

Task types are defined directly in the code.
Task requests are created in Python with an API.
A scheduler (possibly based on dask) accepts a list of task requests and executes them
on the available resources (CPUs, GPUs). It creates and maintains a bunch of task runs.
All task runs are persisted on a TSV log file.

The list of required task requests is obtained by scanning the existing directory for
tasks that need to be run.

"""

# -------------------------------------------------------------------------------------------------
# Imports
# -------------------------------------------------------------------------------------------------

import os
import sys


# -------------------------------------------------------------------------------------------------
# Generic pipelining tools
# -------------------------------------------------------------------------------------------------

def files_exist(paths):
    """Return whether a list of absolute paths exist. The paths may contain a wildcard."""
    # TODO


class TaskType:
    name = None
    input_path_templates = ()
    output_path_templates = ()
    depends_on = ()
    n_cpus = 0
    n_gpus = 0
    io_charge = 0
    priority = None

    def run(self):
        """To override."""
        pass

    def check_completion(self, session_dir):
        """Check whether a task has been completed in a given session directory.
        By default, look whether the output files exist."""


class TaskRequest:
    task_type = None
    input_paths = ()
    output_paths = ()
    n_cpus = 0
    n_gpus = 0
    io_charge = 0
    priority = None


class TaskRun:
    task_request = None
    status = None
    started = None
    completed = None


def get_task_types():
    """Return all registered task types, by looking at the classes deriving from the base class."""
    # use metaclass


def is_session_dir(path):
    """Return whether a directory is a session directory or not."""


def find_session_dirs(roots):
    """Recursively find a list of session dirs in one or several root directories."""


def missing_task_requests(session_dir):
    """Create a list of task requests in a session directory, by scanning the directory and
    finding which tasks have not run yet and thus have not yet created the output files."""


def create_dask_pipeline(task_requests):
    """Create a Dask pipeline from a list of task requests."""
    pass


def run_dask_pipeline(pipeline):
    """Run a Dask pipeline on the local computer, using multiple processes."""


# -------------------------------------------------------------------------------------------------
# IBL tasks
# -------------------------------------------------------------------------------------------------

class ExtractEphysTask(TaskType):
    name = 'extract_ephys'
    input_path_templates = ('raw_ephys_data/probe*/*.ap.bin',)
    output_path_templates = ('/alf/_ibl_trials.*', 'raw_ephys_data/probe*/_spikeglx_sync*')
    n_cpus = 4
    io_charge = 50
    priority = 'high'

    def run(self):
        # TODO
        pass


# -------------------------------------------------------------------------------------------------
# Command-line interface
# -------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    pass
