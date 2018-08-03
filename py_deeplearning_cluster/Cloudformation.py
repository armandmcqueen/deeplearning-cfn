import boto3
import json

class CloudformationStack:
    def __init__(self, stack_name, region):
        self.stack_name = stack_name
        self.region = region
        self.session = boto3.session.Session(region_name=region)
        self.cfn_client = self.session.client("cloudformation")

        self.stack_params = None
        self.stack_capabilities = None
        self.template_str = None
        self.stack_id = None



    def launch(self, stack_params, stack_capabilities, template_str=None, template_json=None):
        self.stack_params = stack_params
        self.stack_capabilities = stack_capabilities
        if template_json is None and template_str is None:
            raise ValueError("One of template_json or template_str must be used")

        if template_str is None:
            template_str = json.dumps(template_json)

        self.template_str = template_str

        res = self.cfn_client.create_stack(StackName=self.stack_name,
                                           TemplateBody=template_str,
                                           Parameters=stack_params,
                                           DisableRollback=False,
                                           Capabilities=stack_capabilities)

        self.stack_id = res["StackId"]





    def manually_set_stack_id(self, stack_id):
        self.stack_id = stack_id

    def wait_for_create_completion(self):
        create_waiter = self.cfn_client.get_waiter('stack_create_complete')
        create_waiter.wait(StackName=self.stack_name)

    def wait_for_delete_completion(self):
        delete_waiter = self.cfn_client.get_waiter('stack_delete_complete')
        delete_waiter.wait(StackName=self.stack_name)

    def does_stack_exists(self):
        try:
            self.cfn_client.describe_stacks(StackName=self.stack_name)
            return True
        except Exception as ex:
            return False
