import py_deeplearning_cluster.RemoteCommands as rc
import subprocess
import random
import os
from string import Template
from py_deeplearning_cluster.SSHAgent import SSHAgentBootstrap





class ClusterShell:
    def __init__(self, cfn_stack_name, session, private_key_str, username="ubuntu", full_init=True):
        self.ssh_agent = SSHAgentBootstrap(private_key_str)
        self.session = session
        self.cloudformation = self.session.client('cloudformation')
        self.autoscaling = self.session.client('autoscaling')
        self.ec2 = self.session.resource('ec2')
        self.username = username
        self.worker_command_template = Template("ssh -A $user@$worker_ip $cmd")


        response = self.cloudformation.describe_stacks(StackName=cfn_stack_name)
        try:
            workerAutoscalingGroup = filter(lambda output: output["OutputKey"] == "WorkerAutoScalingGroup",
                                            response["Stacks"][0]["Outputs"])
            self.worker_autoscaling_group_id = next(workerAutoscalingGroup)["OutputValue"]

            masterAutoscalingGroup = filter(lambda output: output["OutputKey"] == "MasterAutoScalingGroup",
                                            response["Stacks"][0]["Outputs"])
            self.master_autoscaling_group_id = next(masterAutoscalingGroup)["OutputValue"]
        except Exception as e:
            print(e)
            print("ERROR: ClusterShell.init - Unable to find master or worker autoscaling groups. Does this cluster exist?")
            self.shut_down()
            return

        response = self.autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[self.master_autoscaling_group_id])
        masterInstanceId = response["AutoScalingGroups"][0]["Instances"][0]["InstanceId"]
        self.master_ip = self.ec2.Instance(masterInstanceId).public_ip_address

        response = self.autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[self.worker_autoscaling_group_id])
        workerInstanceIds = [instanceInfo["InstanceId"] for instanceInfo in
                             response["AutoScalingGroups"][0]["Instances"]]

        self.worker_ips = [self.ec2.Instance(workerInstanceId).private_ip_address
                           for workerInstanceId in workerInstanceIds]

        self.master_rce = rc.RemoteCommandExecutor(host=self.master_ip, private_key=private_key_str, user=self.username,
                                           enableForwarding=True)

        subprocess.check_output(f'ssh-keyscan {self.master_ip} >> ~/.ssh/known_hosts', shell=True)

        if full_init:
            print("Doing full_init. Starting setup")
            self.master_rce.create_bash_profile()

            add_to_known_host_cmd = Template("ssh-keyscan $ip >> ~/.ssh/known_hosts")
            self.master_rce.run_command(add_to_known_host_cmd.substitute(ip="localhost"))
            for worker_ip in self.worker_ips:
                self.master_rce.run_command(add_to_known_host_cmd.substitute(ip=worker_ip))

            # Create bash profile on each worker. Required for some 'WORKERS' commands
            for worker_ip in self.worker_ips:
                self.run_on_worker("grep export ${HOME}/.bashrc > ${HOME}/.bash_profile", worker_ip)
            print("Done with full_init setup")

        print("#####################################################")
        print("#######    ClusterShell Setup Complete     ##########")
        print("#####################################################")
        print("")

    def rsync_local_dir_to_master(self, local_dir_abspath, remote_dir_abspath, verbose=False):
        if local_dir_abspath[-1] != "/":
            local_dir_abspath += "/"

        cmd = f'rsync -azP {local_dir_abspath} {self.username}@{self.master_ip}:{remote_dir_abspath}'
        if verbose:
            print(f'[rsync_local_dir_to_master | cmd] {cmd}')

        out = subprocess.check_output(cmd, shell=True)
        return out

    def rsync_master_dir_to_workers(self, master_dir_abspath, worker_dir_abspath=None, verbose=False, return_output=False):
        if master_dir_abspath[-1] != "/":
            master_dir_abspath += "/"

        if worker_dir_abspath is None:
            worker_dir_abspath = master_dir_abspath
        else:
            if worker_dir_abspath[-1] != "/":
                worker_dir_abspath += "/"

        outs = []
        for worker_ip in self.worker_ips:
            cmd = f'rsync -azP {master_dir_abspath} {self.username}@{worker_ip}:{worker_dir_abspath}'
            if verbose:
                print(f'[rsync_master_dir_to_workers | cmd] {cmd}')

            out = self.master_rce.run_command(cmd, return_output=return_output)
            if return_output:
                outs.append(out)

        if return_output:
            return outs


    def rsync_local_dir_to_all(self, local_dir_abspath, remote_dir_abspath, verbose=False, return_output=False):
        self.rsync_local_dir_to_master(local_dir_abspath, remote_dir_abspath, verbose=verbose)
        self.rsync_master_dir_to_workers(remote_dir_abspath, verbose=verbose, return_output=return_output)


    def get_worker_ips(self):
        return self.worker_ips

    def get_master_ip(self):
        return self.master_ip

    def run_on_worker(self, cmd, worker_ip, return_output=False, src_dir="~"):
        command = self.worker_command_template.substitute(user=self.username, worker_ip=worker_ip, cmd=cmd)
        out = self.master_rce.run_command(command, src_dir=src_dir, return_output=return_output)
        if return_output:
            return out

    def run_on_master(self, cmd, return_output=False, src_dir="~"):
        out = self.master_rce.run_command(cmd, src_dir=src_dir, return_output=return_output)
        if return_output:
            return out

    def run_on_all(self, cmd, return_output=False, src_dir="~"):
        if return_output:
            outputs = []

        out = self.run_on_master(cmd, return_output=return_output, src_dir=src_dir)

        if return_output:
            outputs.append(('master', out))

        for worker_ip in self.worker_ips:
            out = self.run_on_worker(cmd, worker_ip=worker_ip, return_output=return_output, src_dir=src_dir)
            if return_output:
                outputs.append((worker_ip, out))

        if return_output:
            return outputs

    def copy_to_master(self, local_abs_filepath, remote_abs_filepath):
        cmd = f'scp -r {local_abs_filepath} {self.username}@{self.master_ip}:{remote_abs_filepath}'
        print(f'Running locally: {cmd}')
        out = subprocess.check_output(cmd, shell=True)
        print(out)

    def copy_to_local_from_master(self, remote_abs_path, local_abs_path, verbose=False, return_output=False):
        if local_abs_path.endswith("/."):
            local_abs_dir_path = local_abs_path[:-2]
            os.makedirs(local_abs_dir_path, exist_ok=True)

        cmd = f'scp -r {self.username}@{self.master_ip}:{remote_abs_path} {local_abs_path}'
        if verbose:
            print(f'[scp_to_local_from_master | cmd] {cmd}')

        out = subprocess.check_output(cmd, shell=True)
        if return_output:
            return out


    # Look at a dir_path and filter out commands where 'sudo rm -r {dir_path}' is dangerous
    def sudo_rm_r_dir_looks_dangerous(self, dir_path):
        if not dir_path.startswith('/') and not dir_path.startswith("~/"):
            print("Relative paths can have unpredictable behavior")
            return True

        if dir_path == "~/":
            return True

        if (dir_path.startswith('/') and not (
                dir_path.startswith('/tmp/') or dir_path.startswith('/home/'))):
            return True

        return False

    def copy_to_local_from_all(self, remote_abs_path, local_abs_path, master_staging_path=None, clean_local_path=True, verbose=False):
        if master_staging_path is None:
            master_staging_path = f'/tmp/ClusterShell_stage_{random.randint(0, 1000000)}'

        elif self.sudo_rm_r_dir_looks_dangerous(master_staging_path):
            raise RuntimeError(f'{master_staging_path} will be sudo rm -r\'d. This looks too dangerous')


        # Create/clear master staging folder

        clean_master_stage_dir_cmd = f'sudo rm -r -f {master_staging_path}'
        if verbose: print(f'[copy_to_local_from_all | clean_master_stage_dir cmd] {clean_master_stage_dir_cmd}')
        self.master_rce.run_command(clean_master_stage_dir_cmd)

        create_master_stage_dir_cmd = f'mkdir -p {master_staging_path}'
        if verbose: print(f'[copy_to_local_from_all | create_master_stage_dir_cmd cmd] {create_master_stage_dir_cmd}')
        self.master_rce.run_command(create_master_stage_dir_cmd)

        # Copy master files to staging folder
        copy_master_files_to_stage_dir_cmd = f'cp -r {remote_abs_path} {master_staging_path}'
        if verbose: print(f'[copy_to_local_from_all | copy_master_files_to_stage_dir_cmd cmd] {copy_master_files_to_stage_dir_cmd}')
        self.master_rce.run_command(copy_master_files_to_stage_dir_cmd)


        # On each worker, copy files to master staging folder
        for worker_ip in self.worker_ips:
            copy_worker_files_to_stage_dir_cmd = f'scp -r {self.username}@{worker_ip}:{remote_abs_path} {master_staging_path}'
            if verbose: print(f'[copy_to_local_from_all | copy_worker_files_to_stage_dir_cmd cmd] {copy_worker_files_to_stage_dir_cmd}')
            self.master_rce.run_command(copy_worker_files_to_stage_dir_cmd)

        # Copy master staging folder to local folder
        self.copy_to_local_from_master(master_staging_path, local_abs_path, verbose=verbose)

        # Clean up master staging path
        clean_master_stage_dir_cmd = f'sudo rm -r {master_staging_path}'
        if verbose: print(f'[copy_to_local_from_all | clean_master_stage_dir cmd] {clean_master_stage_dir_cmd}')
        self.master_rce.run_command(clean_master_stage_dir_cmd)



    def copy_to_all(self, local_abs_filepath, remote_abs_filepath):
        self.copy_to_master(local_abs_filepath, remote_abs_filepath)
        for worker_ip in self.worker_ips:
            cmd = f'scp {remote_abs_filepath} {self.username}@{worker_ip}:{remote_abs_filepath}'
            self.run_on_master(cmd)


    def print_hostfile(self, slots=8):
        print(self.get_hostfile_str(slots=slots))

    def get_hostfile_str(self, slots=8):
        s = f'localhost slots={slots}'
        for worker_ip in self.worker_ips:
            s += f'\n{worker_ip} slots={slots}'
        return s

    def local_write_to_file(self, local_abs_filepath, str_content, mode='w+'):
        with open(local_abs_filepath, mode) as f:
            f.write(str_content)

    def print_ssh_to_master_cmd(self):
        print("SSH TO MASTER:")
        print(f'ssh -A {self.username}@{self.master_ip}')

    def shut_down(self):
        print("")
        print("#####################################################")
        print("#######    ClusterShell Shutting Down      ##########")
        print("#####################################################")
        self.ssh_agent.destroy()