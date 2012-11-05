import subprocess
import threading
import logging as log
from time import time


class Task():
    # Initializes with a command to be executed
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None
        # self.outdir = outdir

    def run(self, timeout):
        '''
        Sets a timer and runs the command in a thread
        If execution takes longer than timeout, the thread is killed and the results logged
        Stores stdout and stderr as file descriptors to be processed by write_pipes()
        Checks the exec return code and logs accordingly
        '''
        def target():
            start = time()
            self.process = subprocess.Popen(self.cmd,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE,
                                            shell=True)
            rtime = time() - start
            log.info("Started execution: '%s' with pid=%s",
                     self.cmd, self.process.pid)
            self.write_pipes(self.process.communicate())
            ret = self.process.returncode
            if ret == 0:
                log.info("Successfully completed execution: '%s', runtime=%s",
                         self.cmd, rtime)
            else:
                log.warn("Failed execution: '%s' returned %s",
                         self.cmd, ret)

        thread = threading.Thread(target=target)
        thread.start()
        thread.join(timeout)
        if thread.is_alive():
            log.warn("Terminating '%s' - exceeded timeout=%s",
                     self.cmd, timeout)
            self.process.terminate()
            thread.join()

    def write_pipes(self, pipes):
        '''
        Logs the stdout and stderr from the subprocess command
        Creates an outfile using the pid from the process
        Logs bytes written to each
        '''
        out = "pipes_{}.txt".format(self.process.pid)
        
        try:
            with open(out, 'w') as f:
                f.write("CMD:\n\t{}\n".format(self.cmd))
                f.write("\nSTDERR:\n")
                f.write("\t{}\n".format(pipes[1]))
                log.debug("%s: Wrote %s bytes from stderr to %s",
                          self.cmd, len(pipes[1]), f.name)
                f.write("\nSTDOUT:\n")
                f.write("\t{}\n".format(pipes[0]))
                log.debug("%s: Wrote %s bytes from stdout to %s",
                          self.cmd, len(pipes[0]), f.name)
        except IOError as err:
            log.warning("Unable to write to %s: %s", out, err)
