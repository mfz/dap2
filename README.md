
Data Analysis Pipeline v2
-------------------------

Library that uses a bipartite graph consisting of ProcessNodes and FileNodes
to describe a data analysis pipeline/workflow together with facilities to
allow scheduling of processes locally or on a Slurm cluster.

Requires parse and futures libraries (installable using pip).

fz August 2014


CHANGES:

for rules, inputs and outputs can be dicts
for rules, kind can be 'python', 'shell', 'process'
inputs and outputs always passed to decorated function



~~~ {.python}
import os
import sys
import re
import tempfile
import subprocess
import time
import parse  # parse library. Install using 'pip install parse'



import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)-15s - %(levelname)s - %(module)s.%(funcName)s:%(lineno)s - %(message)s')
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


~~~


Process graph
-------------

Bipartite graph consisting of ProcessNodes and FileNodes
(FileNodes could be generalized to DatabaseNodes etc. All we need is
method exists())



ProcessNodes are used to (implicitly) define the graph, as instantiation of a
ProcessNode automatically creates FileNodes and corresponding edges.
ProcessNodes have an attribute cmd that specifies the shell command
used to execute the process.

~~~~
ProcessNode('hostname > hostname.txt', children = ['hostname.txt'])
~~~~



~~~ {.python}
class ProcessNode(object):

    def __init__(self, cmd, parents = [], children = [], name = 'anonymous', **params):
        """
        create ProcessNode

        parents and children can be lists or dicts
        """
        self.cmd = cmd
        self.parents = [File(p) for p in (parents.values() if type(parents)==dict else parents)]   # input file names
        for parent in self.parents:
            parent.children.append(self)
        self.children = [File(c) for c in (children.values() if type(children)==dict else children)]  # output file names
        # register that this TaskNode creates its children
        for child in self.children:
            assert child.parents == [], "Child can only have one TaskNode as parent"
            child.parents = [self]
        self.name = name
        self.params = params

    def exists(self):
        "return whether all children exits"
        return all([fn.exists() for fn in self.children])

    def delete_results(self):
        "delete files corresponding to node's children"
        for fn in self.children:
            fn.delete()

    def __repr__(self):
        return "<ProcessNode: %s>" % self.name

~~~


To avoid the unnecessary creation of shell scripts, introspection can be
used to create the shell command necessary to run a given python function
together with its positional and keyword arguments. The shell command created is

'sys.executable -E -c "import module; module.function(*args. **kw)"'

This means that the function has to be available when the containing module is imported.
-E flags ignores any PYTHON* environment variables (due to PYTHONPATH set).

Also, as we are using 'import module', the __main__ section of the module is NOT executed.
This makes it possible to define process graph generation (in __main__ section) and
functions in same file.



~~~ {.python}
def shell_command(func, *args, **kw):
    """
    create shell command to call func(*args, **kw)

    uses sys.executable for python interpreter, assuming it is
    reachable from all cluster nodes
    """
    # trick to figure out the name of the __main__ module
    # does not work interactively, due to missing __main__.__file__ attribute
    import __main__
    func_module = (func.__module__ if func.__module__ != '__main__'
                   else os.path.splitext(os.path.basename(__main__.__file__))[0])
    return '%s -E -c "import %s; %s.%s(*%s, **%s)"' %  (sys.executable, func_module, func_module, func.__name__, args, kw)


~~~


PythonProcess is a convenience class to allow creation of a ProcessNode
given a python function and its positional and keyword arguments

~~~~{.python}
import math

p1 = ProcessNode(shell_command(math.sqrt, 2), name = 'Square root')

# basically the same as above
p2 = PythonProcess(math.sqrt, (2,), {}, name = 'Square root')
~~~~



~~~ {.python}
class PythonProcess(ProcessNode):
    "convenience class to create ProcessNode from python function, args and keywords"
    def __init__(self, func, args = (), kw = {}, parents = [], children = [], name = None, **params):
        cmd = shell_command(func, *args, **kw)
        if name is None:
            name = '%s.%s' % (func.__module__, func.__name__)
        ProcessNode.__init__(self, cmd,
                             parents = parents, children = children,
                             name = name, **params)


~~~


Instantiation of FileNodes makes the library aware of files on the file system
(existing or not). Memoization ensures that calls to File(path) always
return the unique FileNode referring to the file specified by path.
Therefore, only File(path) should be used to refer to files.




~~~ {.python}
def memoize(func):
    """
    decorator to memoize functions and  methods
    with arbitrary number of args

    works also for classes, but then class becomes function
    and isinstance does not work anymore ...
    """
    cache = {}
    def wrapper(*args):
        if not cache.has_key(args):
            ret = cache[args] = func(*args)
        else:
            ret = cache[args]
        return ret
    wrapper.__name__ = func.__name__ + ' (memoized)'
    return wrapper




def File(path):
    return File_abs_path(os.path.abspath(path))

~~~

use memoize to ensure that every absolute path
corresponds to unique FileNode

~~~ {.python}
@memoize
def File_abs_path(abspath):
    return FileNode(abspath)

class FileNode(object):
    def __init__(self, path):
        self.path = path
        self._exists = False
        self.parents = []  # ProcessNode creating file
        self.children = [] # ProcessNodes depending on this FileNode


    def exists(self, cache = True, nfs_timeout = None):
        """
        check if file exists
        use cached result if cache is True
        if file does not seem to exits, wait timeout seconds and try again
        (to get around NFS caching behaviour)
        """
        if not (cache and self._exists):
            self._exists = os.path.exists(self.path)
            if (not self._exists) and nfs_timeout:
                timeused = 0.0
                while timeused < nfs_timeout and not self._exists:
                    time.sleep(nfs_timeout/10.)
                    timeused += nfs_timeout/10.
                    self._exists = os.path.exists(self.path)
                    logger.debug('Looking for file %s. Status after %f s: %s' % (self.path, timeused, self._exists))
        return self._exists

    def delete(self):
        "delete underlying file"
        try:
            os.unlink(self.path)
            self._exists = False
        except:
            pass

    def __repr__(self):
        return "<FileNode: %s>" % self.path



~~~


Rules
-----

In order to avoid the manual construction of each and every
dependency between processes and their input and files, rules can be
defined based on filename patterns.  Filename patterns are specified
using the syntax specified in the parse module
[http://pypi.python.org/pypi/parse].  The patterns are transformed
to absolute paths before matching is performed.

If a FileNode is required for which the file does not yet exits, a
pattern match of the absolute file path against all output patterns
in the rule_book is performed. The simplest match (i.e. the one with
the fewest variables) is used.

For automatic rules, a PythonProcess is created using the specified
function with the infiles and outfiles as arguments and match dict
as keyword arguments. The input and output patterns are formatted
using the match dict and used as parents and children of the
ProcessNode, respectively.

For more advanced uses, e.g. when we need to look up some
information during process graph generation time and not at process
execution time, non-automatic rules are provided. For non-automatic
rules, the specified function is called with arguments infiles,
outfiles, and match groups.  It is expected to return a ProcessNode.

~~~~
@rule(['{name}.automatic'])
def greetings_auto(inputs, outputs, name):
    with open(name + '.automatic', 'w') as fh:
        print >> fh, "Hello ", name

def greetings_nonauto(name):
    with open(name + '.nonautomatic', 'w') as fh:
        print >> fh, "Hello ", name

@rule(['{name}.nonautomatic'],
      kind='process')
def greetings_nonauto_(infiles, outfiles, name):
    # here we could look up information in a database etc.
    # this is done at process graph generation time, not process execution time
    return PythonProcess(greetings_nonauto, (name,), {}, children=outfiles, parents=infiles)
~~~~

Any keyword arguments except for 'inputs', 'outputs','kind' are used
to update the params dictionary attribute of the created
ProcessNode.  This can be used, for example, to specify parameters
for the SlurmScheduler (see below).





global variable to store rules

~~~ {.python}
rule_book = []

def rule(outputs, inputs = [], kind = 'python', **processParams):
    """
    decorator to append rule to rule_book

    if kind = 'python', a PythonProcess is created calling the decorated function with
     infiles, outfiles as arguments and match_dict as kwarg. The decorated function is executed
     when scheduled.

    if kind = 'process', the decorated function is called with args infiles, outfiles, match_dict
     at process graph generation time and is supposed to return a ProcessNode. The ProcessNode is
     executed when scheduled.

    if kind = 'shell', the decorated function is called with args infiles, outfiles, match_dict
     at process graph generation time and is supposed to return a shell command. The shell
     command is executed when scheduled.

    infiles and outfiles are created by formatting inputs and outputs using match_dict.
    If dicts are passed as inputs or outputs instead of lists, dicts are created.

    processParams is used to update params attribute of process after its creation
    """
    if kind not in ['python','shell','process']:
        raise Exception("Unknown rule type: %s" % kind)

    def register(func):
        logger.debug('Adding rule %s, %s, %s to rulebook' % (inputs, outputs, func.__name__))
        rule_book.append(dict(inputs=inputs,
                              outputs=outputs,
                              processParams = processParams,
                              kind = kind,
                              func = func))
        return func # decorator needs to return func
    return register



def match_rules(rule_book, path):
    "find simplest (fewest match groups) rule where any outputs pattern matches path"
    matches = []
    for rule in rule_book:
        # allow dict and lis for outputs in rule specification
        patterns = (rule['outputs'].values() if type(rule['outputs']) == dict else rule['outputs'])
        for pattern in patterns:
            m = parse.parse(os.path.abspath(pattern), path)
            if m is not None:
                res = dict(match = m.named)
                res.update(rule)
                matches.append(res)
    if len(matches) == 0:
        return None
    matches.sort(key=lambda match: len(match['match']))
    return matches[0]

~~~


get_required_nodes uses the process graph together with the defined rules
to return a list of all processes that need to be run to ensure that the
provided nodes will exist. If any files for which no rule or process is defined
is required, the function throws an Exception.



~~~ {.python}
def get_required_processes(nodes):
    """
    return list of all processes needed to be run to ensure existence of nodes

    throws Exception if any missing files cannot be created
    """
    tasks = set()
    missing = set()

    to_visit = list(set([x for x in nodes if not x.exists()]))
    seen = set(to_visit)
    while len(to_visit) > 0:
        n = to_visit.pop()
        if isinstance(n, ProcessNode):
            tasks.add(n)
        for x in n.parents:
            if (not x.exists()) and (not x in seen):
                to_visit.append(x)
                seen.add(x)
        if isinstance(n, FileNode) and len(n.parents) == 0:
            # as we are only visiting non-existing nodes,
            # this file does not exist
            # check for any matching rule
            match = match_rules(rule_book, n.path)
            if match is None:
                # if we do not know how to create it
                # mark as missing
                missing.add(n.path)
            else:
                # - instantiate output files based on pattern match
                # - instantiate input files based on pattern match
                # - create Task node connecting input and output files,
                #   passing function and match dict

                # parse library allows specification of patterns like {path:w},
                # but format library does not understand all of these qualifiers
                # hence we remove them before formatting

                if type(match['inputs']) == dict:
                    infiles = dict([(k,re.sub(':.}', '}',v).format(**match['match'])) for k,v in match['inputs'].items()])
                    parents = infiles.values()
                else:
                    infiles = [re.sub(':.}', '}',v).format(**match['match']) for v in match['inputs']]
                    parents = infiles

                if type(match['outputs']) == dict:
                    outfiles = dict([(k,re.sub(':.}', '}',v).format(**match['match'])) for k,v in match['outputs'].items()])
                    children = outfiles.values()
                else:
                    outfiles = [re.sub(':.}', '}',v).format(**match['match']) for v in match['outputs']]
                    children = outfiles

                if match['kind'] == 'python':
                    task = PythonProcess(match['func'], (infiles, outfiles), match['match'],
                                         parents = parents, children = children,
                                         **match['processParams'])
                elif match['kind'] == 'shell':
                    cmd = match['func'](infiles, outfiles, **match['match'])
                    task = ProcessNode(cmd,
                                       parents = parents, children = children,
                                       name = '%s.%s' % (match['func'].__module__, match['func'].__name__),
                                       **match['processParams'])
                    task.params.update(match['processParams'])
                elif match['kind'] == 'process':
                    task = match['func'](infiles, outfiles, **match['match'])
                    task.params.update(match['processParams'])

                if not task in seen:
                    to_visit.append(task)
                    seen.add(task)
                logger.info('Using rule %s : %s -> %s to create %s' % (match['func'].__name__, match['inputs'], match['outputs'], n.path))

    if len(missing) > 0:
        logger.error('No rules to create the following files: %s' % missing)
        raise Exception('No rules to create the following files: %s' % missing)

    return list(tasks)


~~~


Scheduler
---------

Given a list of processNodes,
schedule the processes (either locally or on a cluster)
taking dependencies into account.

- divide the ProcessNodes into ready (to run) and waiting (for other processes)
- submit ready ProcessNodes for execution (up to max number of processes allowed)
- while there are submitted processes:
  - wait for any processes to finish
  - make sure process finished successfully and output files exist
  - go over list of finished processes and check if any dependent process
    changes from waiting to ready due to created output files
    - schedule ready processes (up to max number of processes allowed)




~~~ {.python}
class Scheduler(object):
    """
    scheduler to run processes

    calls execute method of ProcessNode instances
    """
    def __init__(self, processNodeList, n_processes = 4, nfs_timeout = None, continue_on_error = False):

        self.n_processes = n_processes
        self.processNodeList = processNodeList
        self.nfs_timeout = nfs_timeout
        self.continue_on_error = continue_on_error

    def run(self):
        raise NotImplementedError()


~~~


### Local scheduler

The LocalScheduler runs processes on the local machine.
'Local' can also be a cluster machine of course,
such that the LocalScheduler makes it possible to fetch data to a cluster machine first
and then run a pipeline locally.



~~~ {.python}
class LocalScheduler(Scheduler):
    def __init__(self, processNodeList, n_processes = 4, nfs_timeout = None, continue_on_error = False):
        super(LocalScheduler, self).__init__(processNodeList, n_processes=n_processes, nfs_timeout = nfs_timeout,
                                             continue_on_error = continue_on_error)

    def run(self):
        "run scheduled processNodes"
        import futures # backport of concurrency.futures to Python2.x; install 'pip install futures'
        with futures.ThreadPoolExecutor(self.n_processes) as executor:
            completedProcessNodes = set()
            readyProcessNodes = set([processNode for processNode in self.processNodeList
                                    if all([p.exists() for p in processNode.parents])])
            logger.debug("Ready process nodes %s" % readyProcessNodes)

            waitingProcessNodes = set(self.processNodeList).difference(readyProcessNodes)
            logger.debug("Waiting process nodes %s" % waitingProcessNodes)
            # dict to look up which processNode a future processed
            futures_to_processNodes = {}
            incomplete = set()
            for processNode in readyProcessNodes:
                f = executor.submit(subprocess.check_call, 'set -e; set -o pipefail;' + processNode.cmd, shell=True)
                logger.info('Submitting %s' % processNode)
                futures_to_processNodes[f] = processNode
                incomplete.add(f)


            njobs = len(incomplete)
            while incomplete:
                complete, incomplete = futures.wait(incomplete, return_when=futures.FIRST_COMPLETED)

                # update files and dependent tasks of completed tasks
                # add tasks to ready list
                # add tasks from ready list to executor

                # make sure process completed successfully,
                # i.e. no exception occurred
                # and all output files exist
                for f in complete:
                    processNode = futures_to_processNodes[f]
                    if f.exception():
                        logger.error("Error executing %s: %s" % (processNode, f.exception()))
                        processNode.delete_results()
                        if not self.continue_on_error:
                            logger.debug('Cancelling jobs.')
                            for fi in incomplete:
                                fi.cancel()
                            executor.shutdown(wait=False)
                            logger.debug('Executor shut down.')
                            raise Exception("Error executing %s: %s" % (processNode, f.exception()))

                    #processNode.result = f.result()

                    for c in processNode.children:
                        if not c.exists(nfs_timeout = self.nfs_timeout):
                            logger.error("Ouput file %s of process %s not found." % (c.path, processNode))
                            processNode.delete_results()
                            if self.continue_on_error:
                                break
                            else:
                                executor.shutdown(wait=False)
                                raise Exception("Ouput file %s of process %s not found." % (c.path, processNode))

                    completedProcessNodes.add(processNode)

                created_FileNodes = set([fn for f in complete
                                         for fn in futures_to_processNodes[f].children
                                         if fn.exists()])

                dependent_ProcessNodes = set([processNode for fn in created_FileNodes
                                              for processNode in fn.children
                                              if processNode in waitingProcessNodes])
                logger.debug("Dependent process nodes: %s" % dependent_ProcessNodes)
                # dependent process becomes ready when
                # all input files exists and the processes creating them are completed
                readyProcessNodes = [processNode for processNode in dependent_ProcessNodes
                                     if all((fn.exists()
                                             and (fn.parents == []
                                                  or fn.parents[0] in completedProcessNodes)
                                             for fn in processNode.parents))]
                logger.debug("Ready process nodes: %s" % readyProcessNodes)

                for processNode in readyProcessNodes:
                    waitingProcessNodes.remove(processNode)
                    f = executor.submit(subprocess.check_call, 'set -e; set -o pipefail;' + processNode.cmd, shell=True)
                    logger.info('Submitting %s' % processNode)
                    futures_to_processNodes[f] = processNode
                    incomplete.add(f)



~~~


### Slurm scheduler

The SlurmScheduler is used to schedule processes on the Slurm cluster
using the sbatch and sacct command line utilities.

As the processes might be run on different machines in the cluster,
we need to take NFS latency into account: Whne a file is created on one
machine, it can take many seconds before the file become visible on
another machine. The exists methods of FileNode objects therefore has a
parameter nfs_timeout, which specifies for how many seconds the object will
repeatedly check if the file exists.

The SlurmScheduler allows specification of job parameters that are
provided to sbatch on job submission. The default parameters are
given by the argument jobParams on instantiation of the Scheduler.
These default parameters are updated using the
processNode.params['slurmParams'] dictionary for every processNode.

For convenience, the SlurmScheduler also has an argument log_dir,
which specifies to which directory the slurm output and error files
should be directed. This directory will be created if it does not yet exist.
Specifications of output and error options in either jobParams or
processNode.params['slurmParams'] overrides the log_dir settings.

The SlurmScheduler checks for ExitCode 0:0 and the existence of the
output files once a process is done running. In case of an error,
the output files are removed. If continue_on_error is False, an Exception
is thrown.

The argument interval is used to specify the time interval in seconds
when the Slurm cluster is polled for job completion using the
sacct command line tool.



~~~ {.python}
JOB_STATUS_DONE = ['CANCELLED',
                   'COMPLETED',
                   'FAILED',
                   'NODE_FAIL',
                   'PREEMPTED',
                   'TIMEOUT']

def status(jobids):
    """return dict jobid -> sacct info dict

    When call fails (sometimes sacct chokes on database issues),
    repeat until successful, sleeping 10 s between tries.
    """
    success = False
    while not success:
        try:
            out = subprocess.check_output('sacct -j %s -P --format JobID,State,ExitCode,Start,End' %
                                        (','.join([str(j) for j in jobids]),),
                                        shell=True).split('\n')
            success = True
        except subprocess.CalledProcessError:
            logger.warning('Call to sacct failed once again. Retrying in 10 seconds.')
            time.sleep(10)

    res = dict()
    header = out[0].strip().split('|')
    for row in out[1:]:
        if row.strip() != '':
            rowdict = dict(zip(header, row.strip().split('|')))
            if not '.' in rowdict['JobID']:
                res[rowdict['JobID']] = rowdict
    return res


def wait(jobids, interval = 1):
    """
    wait for any of jobids to be done

    returns list of job status dicts for done jobids
    job status dict have the following keys:

    - JobID
    - State
    - ExitCode
    - Start
    - End
    """
    while True:
        s = status(jobids)
        done = [js for js in s.values()
                if js['State'] in JOB_STATUS_DONE or js['State'].startswith('CANCELLED')]
        # take care of 'CANCELLED by xxx'
        if len(done) > 0:
            return done
        time.sleep(interval)


def cancel(jobid, signal='INT'):
    "cancel a Slurm job using signal"
    subprocess.check_call(
        ['scancel', '-s', signal, str(jobid)])


def submit(cmd, **options):
    script = ['#!/bin/bash']
    for key, value in options.items():
        script.append('#SBATCH --%s=%s' % (key, value))
    #script.append('srun %s' % cmd)
    script.append(cmd)

    jobfile = tempfile.NamedTemporaryFile(delete=False)
    jobfile.write('\n'.join(script))
    jobfile.close()

    success = False
    try:
        while not success:
            try:
                out = subprocess.check_output(['sbatch', jobfile.name])
                success = True
            except subprocess.CalledProcessError:
                logger.warning('Call to sbatch failed once again. Retrying in 10 seconds.')
                time.sleep(10)
    finally:
        os.unlink(jobfile.name)

    jobid = re.search(r'job (\d+)', out).group(1)
    return jobid


from collections import defaultdict

class SlurmScheduler(object):
    """
    Scheduler to submit processNodes to Slurm instance using sbatch and sacct

    processNode.params['slurmParams'] is passed as job parameters to sbatch.
    Output of sacct -b is added as processNode.params['slurmStatus'] on completion.

    To execute a list of ProcessNodes on the cluster takeing dependencies into account,
    do

    > s = SlurmScheduler(processNodes)
    > s.run()
    """

    def __init__(self, processNodeList, n_processes = 100, nfs_timeout = 30, log_dir = 'logs', interval = 30,
                 jobParams = {}, continue_on_error = True, db = 'provenance.db'):

        self.n_processes = n_processes
        self.processNodeList = processNodeList
        self.nfs_timeout = nfs_timeout
        self.interval = 10 # polling interval
        self.db = ProvenanceDb(db)

        self.log_dir = log_dir
        if not os.path.exists(self.log_dir):
            os.mkdir(self.log_dir)
            logger.warning('Log directory %s does not exists. Creating it.' % self.log_dir)

        self.jobParams = dict(output = os.path.join(self.log_dir, 'slurm_%j.out'),
                              error = os.path.join(self.log_dir, 'slurm_%j.err'))
        self.jobParams.update(jobParams)
        logger.debug('Initializing SlurmScheduler with jobParams = %s' % self.jobParams)

        self.continue_on_error = continue_on_error
        self.jobid2processNode = {}
        self.readyProcessNodes = set([processNode for processNode in self.processNodeList
                                    if all([p.exists() for p in processNode.parents])])
        self.waitingProcessNodes = set(self.processNodeList).difference(self.readyProcessNodes)

        for processNode in self.readyProcessNodes:
            processNode.params['slurmStatus'] = dict(Status='READY')

        for processNode in self.waitingProcessNodes:
            processNode.params['slurmStatus'] = dict(Status='WAITING')

        self.report_status()

    def submit_ready_nodes(self):
        "submit processes from readyProcessNodes until number of submitted processes reaches n_processes"
        while len(self.jobid2processNode) < self.n_processes and len(self.readyProcessNodes) > 0:
            processNode = self.readyProcessNodes.pop()
            processNode.params['slurmStatus']['Status'] = 'SUBMITTED'
            jobparams = {}
            jobparams.update(self.jobParams)
            jobparams.update(processNode.params.get('slurmParams', {}))
            jobid = submit(processNode.cmd, **jobparams)
            processNode.params['slurmStatus']['JobID'] = jobid
            self.jobid2processNode[jobid] = processNode
            logger.info('Submitted %s as JobID %s.' % (processNode, jobid))
            self.db.insert_process(processNode)
        self.db.commit()

    def run(self):

        logger.debug("Ready process nodes %s" % self.readyProcessNodes)
        logger.debug("Waiting process nodes %s" % self.waitingProcessNodes)

        self.submit_ready_nodes()
        self.report_status()

        while len(self.jobid2processNode) > 0:
            time.sleep(self.interval)
            done_status = wait(self.jobid2processNode.keys(), interval = self.interval)
            complete = []
            for ds in done_status:
                processNode = self.jobid2processNode[ds['JobID']]
                processNode.params['slurmStatus'].update(ds)
                processNode.params['slurmStatus']['Status'] = 'FAILED'
                logger.info('JobID %s [%s] finished with ExitCode %s' % (ds['JobID'], processNode, ds['ExitCode']))
                # check ExitCode
                success = True
                if ds['ExitCode'] != '0:0' or ds['State'] != 'COMPLETED': # fixed 18.9.2015, check for State = COMPLETED
                    success = False
                    logger.error('JobID %s [%s] failed: exit code: %s, state: %s' % (ds['JobID'], processNode, ds['ExitCode'], ds['State']))
                    processNode.delete_results()
                    if not self.continue_on_error:
                        raise Exception('JobID %s [%s] returned non-zero exit code: %s' % (ds['JobID'], processNode, ds['ExitCode']))
                # if successful , check if outputs exist
                if success:
                    for c in processNode.children:
                        if not c.exists(nfs_timeout = self.nfs_timeout):
                            success = False
                            logger.error("Ouput file %s of JobID %s [%s] not found." % (c.path, ds['JobID'], processNode))
                            processNode.delete_results()
                            if self.continue_on_error:
                                break
                            else:
                                raise Exception("Ouput file %s of process %s not found." % (c.path, processNode))

                if success:
                    complete.append(processNode)
                    processNode.params['slurmStatus']['Status'] = 'COMPLETED'
                del self.jobid2processNode[ds['JobID']]

                ## TODO: update status in provenanceDb
                self.db.update_process(processNode)

            self.db.commit()
            created_FileNodes = set([fn for pn in complete
                                     for fn in pn.children
                                     if fn.exists()])

            dependent_ProcessNodes = set([processNode for fn in created_FileNodes
                                          for processNode in fn.children
                                          if processNode in self.waitingProcessNodes])
            logger.debug("Dependent process nodes: %s" % dependent_ProcessNodes)
            # dependent process becomes ready when
            # all input files exists and the processes creating them are completed
            readied = [processNode for processNode in dependent_ProcessNodes
                       if all((fn.exists()
                               and (fn.parents == [] # founder file
                                    or (not fn.parents[0].params.has_key('slurmStatus')) # not registered for running (e.g. created by older run)
                                    or fn.parents[0].params['slurmStatus']['Status'] == 'COMPLETED') # registered and completed
                               for fn in processNode.parents))]
            logger.debug("Readied process nodes: %s" % readied)

            self.readyProcessNodes.update(readied)
            logger.debug("Ready process nodes %s" % self.readyProcessNodes)
            self.waitingProcessNodes.difference_update(readied)
            logger.debug("Waiting process nodes %s" % self.waitingProcessNodes)

            for p in readied:
                p.params['slurmStatus']['Status'] = 'READY'

            self.submit_ready_nodes()

            self.report_status()



        # waitingProcessNodes should be empty now, if everything went well
        if len(self.waitingProcessNodes) != 0:
            logger.error('waitingProcessNodes not empty!')

        #self.dump('processDump.json')
        self.db.close()


    def report_status(self):
        counts = defaultdict(lambda: 0)
        for processNode in self.processNodeList:
            counts[processNode.params['slurmStatus']['Status']] += 1
        states = ['WAITING','READY','SUBMITTED','COMPLETED','FAILED']

        print '-'*79
        print '%35s %10s %10s %10s %10s' % tuple(states)
        print '%24s %10d %10d %10d %10d %10d' % tuple([time.asctime()] + [counts[state] for state in states])

    def dump(self, filename):
        "dump processes to filename"
        import json
        with open(filename, 'w') as fh:
            for p in self.processNodeList:
                tmp = dict(cmd = p.cmd,
                        name = p.name,
                        parents = [f.path for f in p.parents],
                        children = [f.path for f in p.children],
                        params = p.params)
                print >> fh, json.dumps(tmp)


~~~


Command line interface
----------------------

When the library is to be used from the command line (as a Makefile replacement), the following function
can be run to parse the command line and to create any files passed as command line arguments
that do not yet exists using the rules and processes defined in the script.

In this case it is of interest to note that code in the __name__ == '__main__' block is only
executed during process graph generation time, not during process execution time.
This makes it possible to define both process graph generation code and functions
in the same file.



~~~ {.python}
def cli():
    """
    command line interface
    """
    import argparse
    parser = argparse.ArgumentParser(description='Data analysis pipeline')
    parser.add_argument('-p', '--numproc', default=1, help='Number of processes to run simultaneously')
    parser.add_argument('-l', '--logdir', default = 'logs', help='Directory for cluster logs (.out and .err files)')
    parser.add_argument('-c', '--cluster', action='store_true', help='Execute processes on cluster')
    parser.add_argument('-j', '--jobparams', default='', help='Cluster parameters, comma separated option=value pairs, passed to scheduler')
    parser.add_argument('-n', action='store_true', help='Do not run processes. Only print them.')
    parser.add_argument('targets', nargs='+', help='files to be created')
    args = parser.parse_args()
    print args
    processes = get_required_processes([File(f) for f in args.targets])

    if args.n:
        for process in processes:
            print process.name
    else:
        if args.cluster:
            try:
                jobParams = {} if args.jobparams == '' else dict([tuple(j.split('=')) for j in args.jobparams.split(',')])
            except:
                logger.error('Malformed jobparams: %s' % args.jobparams)
                sys.exit(2)
            s = SlurmScheduler(processes, n_processes = int(args.numproc), log_dir=args.logdir, jobParams=jobParams)
        else:
            s = LocalScheduler(processes, n_processes = int(args.numproc))
        s.run()



~~~


Provenance
----------

All executed processes and their input and output files
are stored in a SQLite database

processes: id, cmd, name, params, job_id, status, exit_code, start_time, end_time
files: id, path, process_id
process_parents: process_id, file_id
process_children: process_id, file_id



~~~ {.python}
import sqlite3

class ProvenanceDb(object):
	"Database to store which processes were run to create files"

	def __init__(self, filename):
		"connect to database and create tables if they don't exist"
		self.filename = filename
		self.con = sqlite3.connect(filename)
		ddl = '''
		        create table if not exists files(id integer primary key,
				                                 path text unique,
												 process_id integer);

				create table if not exists processes(id integer primary key,
                                                     cmd text,
                                                     name text,
                                                     params text,
				                                     job_id integer,
													 status text,
                                                     exit_code text,
													 start_time text,
													 end_time text);

                create index if not exists idx_processes on processes(job_id);

			    create table if not exists process_parents(process_id integer,
				                                   file_id integer);

				create table if not exists process_children(process_id integer,
	                                                file_id integer);
				'''

		self.con.executescript(ddl)
		self.commit()

	def insert_file(self, path):
		"insert path into files table and return file_id"
		self.con.execute("insert or ignore into files(path) values (?)", (path,))
		c = self.con.execute("select id, path from files where path = ?", (path,))
		return c.fetchone()[0]

	def insert_process(self, process):
		"insert process and return process id"

		import json
		c = self.con.cursor()
		c.execute('''insert into processes(cmd, name, params, job_id, status)
	             values(?,?,?,?,?)''', (process.cmd ,
                                                 process.name,
                                                 json.dumps(process.params),
                                                 int(process.params['slurmStatus']['JobID']),
                                                 process.params['slurmStatus']['Status']))
		c.execute('select id from processes where rowid = ?', (c.lastrowid,))
		process_id = c.fetchone()[0]

		parent_ids = [self.insert_file(f.path) for f in process.parents]
		children_ids = [self.insert_file(f.path) for f in process.children]

		c.executemany('insert into process_parents(process_id, file_id) values(?,?)',
				  [(process_id, file_id) for file_id in parent_ids])
		c.executemany('insert into process_children(process_id, file_id) values (?,?)',
				  [(process_id, file_id) for file_id in children_ids])

		c.execute("update files set process_id = %d where id in (%s)" %
			  (process_id, ','.join([str(s) for s in children_ids])))
		c.close()
		return process_id

	def update_process(self, process):
		"update process status and end_time"
		import json
		c = self.con.cursor()
		c.execute('update processes set status=?, exit_code=?, start_time=?, end_time=?, params=? where job_id=?',
				  (process.params['slurmStatus']['Status'],
                   process.params['slurmStatus']['ExitCode'],
				   process.params['slurmStatus']['Start'],
				   process.params['slurmStatus']['End'],
				   json.dumps(process.params),
				   int(process.params['slurmStatus']['JobID']))
				 )
		c.close()

	def commit(self):
		"commit to database"
		self.con.commit()

	def close(self):
		"commit to database and close connection"
		self.con.commit()
		self.con.close()





~~~


Utilities
---------



Sometimes we need the output of a process to be able to build the process graph.
E.g. we might need to get some information from a database.
To make sure that that information exists, we can use the require function.



~~~ {.python}
def require(filenames, scheduler = LocalScheduler, **scheduler_options):
    """
    ensure that filenames exist
    create them using dependencies from process graph using scheduler
    """
    tasks = get_required_processes([File(f) for f in filenames])
    s = scheduler(tasks, **scheduler_options)
    s.run()

createTargets = require

def shell_iter(cmd):
    "execute cmd in shell and return output iterator"
    pipefail = 'set -e; set -o pipefail;'
    proc = subprocess.Popen(pipefail + cmd, bufsize=-1, shell=True, close_fds=True,
                            executable=os.environ.get('SHELL', None),
                            stdout=subprocess.PIPE)
    for line in proc.stdout:
        yield line
    retcode = proc.wait()
    if retcode:
        raise subprocess.CalledProcessError(retcode, cmd)

def shell(cmd):
    "execute cmd in shell and return output"
    pipefail = 'set -e; set -o pipefail;'
    return subprocess.check_output(pipefail + cmd, shell=True, close_fds=True,
                                   executable=os.environ.get('SHELL', None))


def read_config(config_file):
    """
    read config file and return as dict[item] = value
    """
    cfg = dict()
    with open(config_file, 'r') as fh:
        for line in fh:
            if line.strip() == '' or line.startswith('#'):
                continue
            else:
                key, value = line.strip().split('=',1)
                cfg[key.strip()] = value.strip()
    return cfg

def ensure_dir(path):
    """
    make sure path exists
    """
    if not os.path.exists(path):
        os.makedirs(path)

~~~

