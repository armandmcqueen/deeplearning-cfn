import os
import boto3
from os.path import abspath
from shutil import copyfile

import py_deeplearning_cluster.KeyPair as KeyPair
from py_deeplearning_cluster.ClusterTemplate import ClusterTemplate
from py_deeplearning_cluster.Cloudformation import CloudformationStack
from py_deeplearning_cluster.ClusterShell import ClusterShell

SLOTS_PER_NODE = 8

## Run Params
REGION = "us-east-1"
STACK_NAME = f'armand-faster-rcnn'

## Credentials
PRIVATE_KEY_PATH = f'/Users/armandmcqueen/.ssh/{REGION}-armandmcqueen-key.pem'
private_key = KeyPair.local_private_key(PRIVATE_KEY_PATH)
aws_session = boto3.session.Session(region_name=REGION)



################################################################################################
#########           Experiment Control
################################################################################################
PRINT_LOGS_TO_FILE = True
HFUSION_THRESHOLD_MB = 8
HTIMELINE_PATH = "~/htimeline.json"
HOST_COUNT = 16
NUM_PROCESSES = HOST_COUNT * SLOTS_PER_NODE
SYNC_BN = True

RESULTS_FOLDER = "4node-fusion_8mb-syncBN"

RUN = {
    "ManualCommand": False,
    "FullInitShell": False, # Shell INIT sets up ssh agent, adds workers to master's known_hosts and creates bash profile for workers. Will always INIT when creating new cluster
    "SyncClusterToMatchLocalTensorpack": True,
    "GenerateAndSyncHostfile": False,
    "GenerateHorovodCommand": True,
    "DownloadTensorpackResults": False,
}
################################################################################################
################################################################################################


################################################################################################
#########           Create New Cluster if Stack Doesn't Exist
################################################################################################
CREATE_NEW_STACK = not CloudformationStack(STACK_NAME, REGION).does_stack_exists()

if CREATE_NEW_STACK:
    c = ClusterTemplate()
    c.armand_default(region=REGION)

    AMI_ID = "ami-d1c9cdae"  # us-east-1 Ubuntu DLAMI 12

    c.update_image_mapping("Ubuntu", REGION, AMI_ID)
    c.set_param("WorkerCount", "7")
    # c.pretty_print()

    cstack = CloudformationStack(region=REGION, stack_name=STACK_NAME)
    cstack.launch(c.params, c.capabilities, template_json=c.template)
    cstack.wait_for_create_completion()

    # When creating a new Cluster, the one-time setup commands cannot be skipped
    RUN["FullInitShell"] = True
    RUN["SyncClusterToMatchLocalTensorpack"] = True
    RUN["GenerateAndSyncHostfile"] = True


################################################################################################
#########           Setup Cluster Shell (Repeatable)
################################################################################################
# Attach to cluster
sh = ClusterShell(STACK_NAME, aws_session, private_key, full_init=RUN["FullInitShell"])


################################################################################################
#########           Create new Hostfile
################################################################################################

if RUN["GenerateAndSyncHostfile"]:
    hostfile_abspath = abspath(f'./tmp/hostfile')
    sh.local_write_to_file(hostfile_abspath, sh.get_hostfile_str(slots=SLOTS_PER_NODE))
    sh.copy_to_master(hostfile_abspath, "/home/ubuntu/hostfile")

else:
    print("SKIPPING NEW HOSTFILE. New hostfile will not be created and remote hostfile will not be touched.\n")



################################################################################################
#########           Sync Cluster Tensorpack to Match Local Changes
################################################################################################

if RUN["SyncClusterToMatchLocalTensorpack"]:
    sh.rsync_local_dir_to_all("/Users/armandmcqueen/PycharmProjects/tensorpack/tensorpack/",
                              "/home/ubuntu/tensorpack/tensorpack", verbose=True)
    sh.rsync_local_dir_to_all("/Users/armandmcqueen/PycharmProjects/tensorpack/examples/",
                              "/home/ubuntu/tensorpack/examples", verbose=True)

    # sh.run_on_all("/home/ubuntu/anaconda3/envs/tensorflow_p36/bin/pip uninstall -y tensorpack")
    sh.run_on_all("/home/ubuntu/anaconda3/envs/tensorflow_p36/bin/pip install --ignore-installed -e /home/ubuntu/tensorpack/")

else:
    print("SKIPPING CLUSTER SYNC. Tensorpack code will not be updated.\n")

################################################################################################
#########           Generate Experiment Horovod Command
################################################################################################

if RUN["GenerateHorovodCommand"]:

    if HOST_COUNT:
        if HOST_COUNT * SLOTS_PER_NODE != NUM_PROCESSES:
            raise RuntimeError("Process count doesn't match slots")

    data_dir = "/home/ubuntu/data/coco"
    train_py_path = "/home/ubuntu/tensorpack/examples/FasterRCNN/train.py"

    if HOST_COUNT:
        host_list = ['localhost'] + sh.worker_ips
        host_list = host_list[:HOST_COUNT]

    cmd = f'''{f'HOROVOD_TIMELINE={HTIMELINE_PATH}' if HTIMELINE_PATH else ""} \\
{f'HOROVOD_FUSION_THRESHOLD={1024 * 1024 * HFUSION_THRESHOLD_MB}' if HFUSION_THRESHOLD_MB else ""} \\
/usr/local/mpi/bin/mpirun -np {NUM_PROCESSES} \\
{f'--host {",".join(host_list)}' if HOST_COUNT else ""} \\
--mca plm_rsh_no_tree_spawn 1 \\
--hostfile ~/hostfile \\
-bind-to none -map-by slot \\
-x NCCL_MIN_NRINGS=8 -x NCCL_DEBUG=INFO \\
-x LD_LIBRARY_PATH -x PATH \\
{'-x HOROVOD_TIMELINE' if HTIMELINE_PATH else ""} \\
{'-x HOROVOD_FUSION_THRESHOLD' if HFUSION_THRESHOLD_MB else ""} \\
-mca pml ob1 -mca btl ^openib \\
{f'--output-filename horovod-logs-{POSTFIX}/horovod-worker.log' if PRINT_LOGS_TO_FILE else ""} \\
python {train_py_path} \\
    --config MODE_MASK=False DATA.BASEDIR={data_dir} \\
    BACKBONE.WEIGHTS=/home/ubuntu/data/pretrained-models/ImageNet-R50-AlignPadding.npz \\
    {'BACKBONE.NORM=SyncBN' if SYNC_BN else ''} \\
    TRAINER=horovod
'''

    cmd = cmd.replace("\n \\\n", "\n")  # Remove ugly empty lines

    print("--------------------------------------------")
    print("GenerateHorovodCommand")
    print("")
    sh.print_ssh_to_master_cmd()
    print("")
    print("screen")
    print("")
    print("source activate tensorflow_p36")
    print("")
    print(cmd)
    print("--------------------------------------------")

################################################################################################
#########           Pull HTimeline, Train Logs and Stats
################################################################################################
if RUN["DownloadTensorpackResults"]:
    print("--------------------------------------------")
    print("DownloadTensorpackResults")

    base_results_folder = "/Users/armandmcqueen/PycharmProjects/deeplearning-cfn-horovod-cluster/horovod_timeline/frcnn/"
    info_template_path = os.path.join(base_results_folder, "info")
    if RESULTS_FOLDER is None:
        raise RuntimeError("RESULTS_FOLDER cannot be None")
    results_folder_abspath = os.path.join(base_results_folder, RESULTS_FOLDER)

    # Create results folder
    if not os.path.isdir(results_folder_abspath):
        print(f'Creating results folder {results_folder_abspath}')
        os.mkdir(results_folder_abspath)

    # Create info doc
    local_info_path = os.path.join(results_folder_abspath, "info")
    if os.path.exists(local_info_path):
        print(f'Removing results info that already exists {local_info_path}')
        os.remove(local_info_path)
        copyfile(info_template_path, local_info_path)

    # Download train logs
    remote_train_log_path = "/home/ubuntu/train_log/maskrcnn/*"
    local_train_log_path = os.path.join(RESULTS_FOLDER, "train_log/.")
    print(f'Downloading train_log/ to {local_train_log_path}')
    sh.copy_to_local_from_master(remote_train_log_path, local_train_log_path)

    # Download ALL Horovod worker logs
    remote_horovod_logs_path = f'/home/ubuntu/horovod-logs/*'
    local_horovod_logs_path = os.path.join(base_results_folder, "hlogs/")
    print(f'Downloading horovod-logs/ to {local_horovod_logs_path}')
    sh.copy_to_local_from_all(remote_horovod_logs_path, local_horovod_logs_path)

    # Download Horovod timeline
    # (Skipping as too big to download locally)
    #
    # remote_htimeline_path = HTIMELINE_PATH.replace("~/", "/home/ubuntu/")
    # local_htimeline_path = os.path.join(results_folder_abspath, "full_htimeline.json")
    # print(f'Downloading htimeline to {local_htimeline_path}')
    # sh.copy_to_local_from_master(remote_htimeline_path, local_htimeline_path)

    print("--------------------------------------------")

sh.shut_down()
