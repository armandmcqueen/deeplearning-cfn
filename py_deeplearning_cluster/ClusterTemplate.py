import boto3
import json
import os
from py_deeplearning_cluster import aws_utils


class ClusterTemplate:
    def __init__(self, template_rel_path="../cfn-template/deeplearning.template", template_json=None, template_url=None, template_file_obj=None, cfn_params=None):
        self._cloudformation_client = None
        if template_json is not None:
            self._template = template_json
        elif template_url is not None:
            # TODO: Implement pull from http/https/s3
            raise NotImplementedError("Template URL is not yet implemented")
        elif template_file_obj is not None:
            self._template = json.load(template_file_obj)
        else:
            dir_path = os.path.dirname(os.path.realpath(__file__))
            abs_path = os.path.abspath(os.path.join(dir_path, template_rel_path))
            # print(f'Loading template file from {abs_path}')
            with open(abs_path, 'r') as template:
                self._template = json.load(template)

        if cfn_params is None:
            self._params = []
        else:
            if type(cfn_params[0] == tuple):
                self._params = []
                self.add_params(cfn_params)
            elif type(cfn_params[0] == dict):
                for param in cfn_params:
                    if "ParameterKey" not in param.keys() or "ParameterValue" not in param.keys():
                        raise ValueError("Dictionary-type cfn_params must have both a 'ParameterKey'"
                                         "field and a 'ParameterValue' field. Received:\n"
                                         "{}".format(json.dumps(param, indent=4)))
                self._params = cfn_params
            else:
                raise ValueError("Unrecognized type in cfn_params")

        self._capabilities = ['CAPABILITY_NAMED_IAM']


    def pretty_print(self):
        print("######################################################")
        print("###########   CLOUDFORMATION TEMPLATE   ##############")
        print("######################################################")
        print(json.dumps(self._template, indent=4))
        print("######################################################")
        print("###########    CLOUDFORMATION PARAMS    ##############")
        print("######################################################")
        print(json.dumps(self._params, indent=4))
        print("######################################################")

    def __str__(self):
        rep = {
            "Template": self._template,
            "Params": self._params
        }
        return json.dumps(rep, indent=4)

    @property
    def template(self, return_str=False):
        # Note: returns clone
        template_str = json.dumps(self._template)
        if return_str:
            return template_str
        else:
            return json.loads(template_str)

    @property
    def params(self):
        # Note: returns clone
        return self._params.copy()

    @property
    def capabilities(self):
        # Note: returns clone
        return self._capabilities.copy()

    def add_param(self, param_key, param_value):
        self._params.append({"ParameterKey": param_key, "ParameterValue": param_value})

    def add_params(self, param_tuple_list):
        for (param_key, param_value) in param_tuple_list:
            self.add_param(param_key, param_value)


    def set_param(self, param_key, param_value):
        for param in self._params:
            if param["ParameterKey"] == param_key:
                param["ParameterValue"] = param_value
                return

        self.add_param(param_key, param_value)

    def update_image_mapping(self, image_type, region, ami_id):
        self._template["Mappings"][image_type][region] = {"AMI": ami_id}

    def update_image_mappings(self, new_mappings):
        for (image_type, region, ami_id) in new_mappings:
            self.update_image_mapping(image_type, region, ami_id)

    def set_image_mapping_to_dlami(self, image_type, region, dlami_version="latest"):
        dlami_id = aws_utils.get_dlami_ami_id(image_type, region, dlami_version)
        self.update_image_mapping(image_type, region, dlami_id)

    def set_image_mappings_to_dlami(self,
                                    image_types=("Ubuntu", "AmazonLinux"),
                                    regions=("us-east-1", "us-west-2", "eu-west-1", "us-east-2",
                                             "ap-southeast-2", "ap-northeast-1", "ap-northeast-2",
                                             "ap-south-1", "eu-central-1", "ap-southeast-1"),
                                    dlami_version="latest",
                                    verbose=False):
        for image_type in image_types:
            if verbose: print("[set_image_mappings_to_dlami] {}".format(image_type))
            for region in regions:
                if verbose: print("[set_image_mappings_to_dlami] {}".format(region))
                self.set_image_mapping_to_dlami(image_type, region, dlami_version=dlami_version)


    def add_az_parameter(self, default=None):

        self._template["Parameters"]["AvailabilityZone"] = {
            "Description": "The availability zone for your instances to be launched in..",
            "Type": "AWS::EC2::AvailabilityZone::Name"
        }
        if default is not None:
            self._template["Parameters"]["AvailabilityZone"]["Default"] = default

        self._template["Resources"]["PrivateSubnet"]["Properties"]["AvailabilityZone"] = {"Ref": "AvailabilityZone"}


    def add_placement_group(self, strategy="cluster"):
        self._template["Resources"]["PlacementGroup"] = {
            "Type": "AWS::EC2::PlacementGroup",
            "Properties": {
                "Strategy": strategy
            }
        }

        self._template["Resources"]["MasterAutoScalingGroup"]["Properties"]["PlacementGroup"] = {
            "Ref" : "PlacementGroup"
        }

        self._template["Resources"]["WorkerAutoScalingGroup"]["Properties"]["PlacementGroup"] = {
            "Ref": "PlacementGroup"}


    def set_delete_efs_on_finish(self, delete=True):
        policy = "Delete" if delete else "Retain"
        self._template["Resources"]["FileSystem"]["DeletionPolicy"] = policy


    def add_cidr_block_to_admin_ssh_sg(self, cidr_ip):
        self._template["Resources"]["AdminSSHSecurityGroup"]["Properties"]["SecurityGroupIngress"].append({
                "IpProtocol": "tcp",
                "FromPort": "22",
                "ToPort": "22",
                "CidrIp": cidr_ip
            })


    def add_cidr_blocks_to_admin_ssh_sg(self, cidr_blocks):
        for cidr_block in cidr_blocks:
            self.add_cidr_block_to_admin_ssh_sg(cidr_block)

    def remove_ssh_param(self):
        del self._template["Parameters"]["SSHLocation"]

        # Resources.AdminSSHSecurityGroup.Properties.SecurityGroupIngress
        existing_admin_ssh_sg_ingress_list = self._template["Resources"]["AdminSSHSecurityGroup"]["Properties"]["SecurityGroupIngress"]

        new_ingress_list = []
        for ingress in existing_admin_ssh_sg_ingress_list:
            if type(ingress["CidrIp"] == dict):
                if "Ref" in ingress["CidrIp"].keys():
                    if ingress["CidrIp"]["Ref"] == "SSHLocation":
                        continue

            new_ingress_list.append(ingress)
        self._template["Resources"]["AdminSSHSecurityGroup"]["Properties"]["SecurityGroupIngress"] = new_ingress_list





    def armand_default(self, region="us-west-2", image_type="Ubuntu"):
        self.set_image_mappings_to_dlami(verbose=True, regions=[region], image_types=[image_type])
        self.add_az_parameter()
        self.add_placement_group()
        self.set_delete_efs_on_finish(delete=True)

        def load_json_from_relative_path(rel_path):
            dir_path = os.path.dirname(os.path.realpath(__file__))
            abs_path = os.path.abspath(os.path.join(dir_path, rel_path))
            with open(abs_path, 'r') as json_file:
                json_json = json.load(json_file)
            return json_json


        self.remove_ssh_param()
        self.add_cidr_blocks_to_admin_ssh_sg(load_json_from_relative_path("../armand_cidr_blocks.json")["cidr_blocks"])

        favorite_azs = load_json_from_relative_path("../armand_favorite_azs.json")
        if region not in favorite_azs.keys():
            raise RuntimeError(f'No favorite AZ is defined for region {region}')

        az = favorite_azs[region]
        key_name = f'{region}-armandmcqueen-key'


        self.add_params([
            ("ImageType", "Ubuntu"),
            ("KeyName", key_name),
            ("InstanceType", "p3.16xlarge"),
            ("WorkerCount", "3"),
            ("AvailabilityZone", az)
        ])




