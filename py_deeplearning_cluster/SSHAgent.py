import subprocess
import os
import signal
from sys import platform

class SSHAgentBootstrap:
    def __init__(self, private_key_str):
        # TODO: if platform == "darwin" dont start new agent

        try:
            agentExports = subprocess.check_output(['ssh-agent', ])
            exportRaw = str(agentExports).replace('\n', '')
            self.exportList = exportRaw.split(';')
            self.exportSSHAgentEnvironment(self.exportList)

            sshAddProcess = subprocess.Popen(['ssh-add', '-', ],
                                             env={'SSH_AUTH_SOCK': self.exportList[0].split('=')[1]},
                                             stdin=subprocess.PIPE, )
            sshAddProcess.communicate(private_key_str.encode('ascii'))
            waitResult = sshAddProcess.wait()
            assert waitResult == 0
            self._agentPID = int(self.exportList[2].split('=')[1])

        except Exception as bootstrapException:
            print ("Error : Exception in SSHAgentBootstrap {}".format(str(bootstrapException)))
            raise

    def destroy(self):
        if self._agentPID != None:
            # print("Terminating ssh agent")
            os.kill(self._agentPID, signal.SIGKILL)

    def exportSSHAgentEnvironment(self, exportList):
        authsock = exportList[0].split('=')
        authagentpid = exportList[2].split('=')
        os.environ[authsock[0]] = authsock[1]
        os.environ[authagentpid[0]] = authagentpid[1]