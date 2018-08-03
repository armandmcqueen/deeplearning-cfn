import boto3

def get_dlami_ami_id(image_type, region, version="latest"):

    if image_type.lower() == 'ubuntu':
        search_image_type = "Ubuntu"
    elif image_type.lower() == "amazonlinux" or image_type.lower() == "amazon linux":
        search_image_type = "Amazon Linux"
    else:
        raise ValueError("Unknown image type {}".format(image_type))

    search_term = "Deep Learning AMI ({}) Version ".format(search_image_type)
    if version != "latest":
        search_term += version

    ec2 = boto3.session.Session(region_name=region).client('ec2')
    res = ec2.describe_images(ExecutableUsers=["all"],
                              Filters=[
                                  {
                                      'Name': 'name',
                                      'Values': [search_term+"*"]
                                  },
                                  {
                                      'Name': 'owner-alias',
                                      'Values': ['amazon']
                                  }])


    # print(json.dumps(res["Images"], indent=4))

    if version == "latest":
        versions = []
        ami_ids = {}

        for image in res["Images"]:
            ver = image["Name"].replace(search_term, "").strip()
            ami_id = image["ImageId"]
            float_ver = float(ver)
            ver_str_id = str(round(float_ver,2))
            versions.append(float_ver)
            ami_ids[ver_str_id] = ami_id

        max_ver_float = max(versions)
        max_ver_str_id = str(round(max_ver_float, 2))

        ami_id = ami_ids[max_ver_str_id]
        # print(max_ver_str_id, ami_id)
        return ami_id
    else:
        image = res["Images"][0]
        ami_name = image["Name"]
        ami_id = image["ImageId"]
        # print(ami_name, ami_id)
        return ami_id