from fabric.api import execute, task, run, cd, sudo, get, env


# Common constants for using Fabric
OUTPUT_PREFIX = False
FABRIC_KEEP_ALIVE = 30
FABRIC_CONNECTION_ATTEMPTS = 3

class RemoteCommandExecutionError(Exception):
    def __init__(self,exceptionMessage):
        self._exception_message = exceptionMessage
    
    @property
    def message(self):
        return self._exception_message

def abortHandler(exceptionMessage ):
    return RemoteCommandExecutionError(exceptionMessage)



class RemoteCommandExecutor:
    """
        Helper class to run commands on a remote host. Uses Fabric under the
        hood to manage remote command execution.
    """

    def __init__(self, host, private_key, user, enableForwarding =False,output_prefix=OUTPUT_PREFIX,
            keepalive=FABRIC_KEEP_ALIVE,
            connection_attempts=FABRIC_CONNECTION_ATTEMPTS):

        self.host = host
        self.host_string = '{}@{}'.format(user, host)
        self._setup_env(private_key, user, output_prefix, keepalive, connection_attempts)
        env.abort_exception = abortHandler
        env.forward_agent = enableForwarding

    def create_bash_profile(self):
        """"
            This command extracts the environment variable from .bashrc and creates the .bash_profile.
            This is required so that env variables are available for the non-interactive shells opened by
            fabric
        """
        execute(self._run_custom_command, "grep export ${HOME}/.bashrc > ${HOME}/.bash_profile", host=self.host_string)

    def run_command(self, command_to_run, src_dir="~", return_output=False):
        """
            Runs command_to_run in src_dir directory on the remote machine.
            Returns the output from the command run.
        """
        output = execute(self._run_command, command_to_run, src_dir, host=self.host_string)
        if return_output:
            return output


    def _run_command(self, command_to_run, src_dir):
        with cd(src_dir):
            return run(command_to_run)

    def _run_sudo_command(self, command_to_run):
        sudo("{}".format(command_to_run))


    def _run_custom_command(self, command_to_run):
        return run("{}".format(command_to_run))

    def _setup_env(self, private_key, user, output_prefix, keepalive, connection_attempts):
          """
              Setup parameters for fabric.
          """
          env.key = private_key
          env.user = user
          env.output_prefix = output_prefix
          env.keepalive = keepalive
          env.connection_attempts = connection_attempts



