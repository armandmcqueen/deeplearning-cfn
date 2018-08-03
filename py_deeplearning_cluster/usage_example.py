import boto3
import py_deeplearning_cluster.KeyPair as KeyPair
from py_deeplearning_cluster.ClusterTemplate import ClusterTemplate
from py_deeplearning_cluster.Cloudformation import CloudformationStack
from py_deeplearning_cluster.ClusterShell import ClusterShell



REGION = "us-east-1"
STACK_NAME = 'armand-faster-rcnn-cloudformation-cluster'

## Credentials
private_key = KeyPair.local_private_key(f'/Users/armandmcqueen/.ssh/{REGION}-armandmcqueen-key_READABLE_COPY.pem') # key must have read permissions
aws_session = boto3.session.Session(region_name=REGION)


# Create a new stack only if stack with that name does not already exist in that region
CREATE_NEW_STACK = not CloudformationStack(STACK_NAME, REGION).does_stack_exists()

if CREATE_NEW_STACK:
    # Load cfn-template/deeplearning.template and make alterations

    c = ClusterTemplate()
    c.add_az_parameter()
    c.add_placement_group()
    c.set_delete_efs_on_finish(delete=True)
    c.remove_ssh_param()
    c.add_cidr_block_to_admin_ssh_sg('XXX.XXX.XXX/XX')

    AMI_ID = "ami-d1c9cdae"  # us-east-1 Ubuntu DLAMI 12
    c.update_image_mapping("Ubuntu", REGION, AMI_ID)

    c.add_params([
        ("ImageType", "Ubuntu"),
        ("KeyName", f'{REGION}-armandmcqueen-key'),
        ("InstanceType", "p3.16xlarge"),
        ("WorkerCount", "1"),
        ("AvailabilityZone", 'us-east-1b')
    ])

    # c.pretty_print() # Look at new template JSON

    cstack = CloudformationStack(region=REGION, stack_name=STACK_NAME)
    cstack.launch(c.params, c.capabilities, template_json=c.template)
    cstack.wait_for_create_completion()


# Set up ClusterShell: get IPs, set up passwordless ssh between nodes, set up environment variables for commands run over ssh
sh = ClusterShell(STACK_NAME, aws_session, private_key, full_init=CREATE_NEW_STACK)

sh.run_on_all("echo 'Running this command on every node'")

sh.rsync_local_dir_to_all("/Users/armandmcqueen/code", "/home/ubuntu/code") # rsync local code to all nodes




## Other ClusterShell commands

# sh.get_master_ip()
# sh.get_worker_ips()


# sh.run_on_master(cmd)
# sh.run_on_worker(cmd, worker_ip)
# sh.run_on_all(cmd) # Run cmd on all nodes


# sh.rsync_local_dir_to_all(self, local_dir_abspath, remote_dir_abspath)
# sh.rsync_local_dir_to_master(local_dir_abspath, remote_dir_abspath)
# sh.rsync_master_dir_to_workers(self, master_dir_abspath)


# sh.copy_to_local_from_master(remote_abs_path, local_abs_path)
# sh.copy_to_local_from_all(remote_abs_path, local_abs_path)


# sh.copy_to_master(local_abs_filepath, remote_abs_filepath)
# sh.copy_to_all(local_abs_filepath, remote_abs_filepath)






sh.shut_down() # Kill the ssh-agent
