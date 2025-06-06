import json
from argparse import ArgumentParser

import boto3


def get_cf_outs_as_env(stack_names, out_file):
    cf_client = boto3.client('cloudformation')
    for stack_name in stack_names.split(","):
        response = cf_client.describe_stacks(StackName=stack_name)
        outputs = response["Stacks"][0]["Outputs"]
        with open(out_file, "a") as _env:
            for output in outputs:
                out_key = output.get("ExportName", output["OutputKey"])
                out_key = out_key.split(f"{stack_name}-")[-1]
                out_key = f"VEDA_{out_key.replace('-', '_').upper()}"
                out_value = output["OutputValue"]
                _env.write(f"{out_key}={out_value}\n")


def get_secrets_as_env(secret_id, out_file):
    sm_client = boto3.client('secretsmanager')
    response = sm_client.get_secret_value(
        SecretId=secret_id
    )
    secrets = json.loads(response['SecretString'])
    with open(out_file, "a") as _env:
        for out_key in secrets:
            out_value = secrets[out_key]
            _env.write(f"{out_key}={out_value}\n")


def generate_env_file(secret_id, stack_names=None, out_file=".env"):
    if stack_names:
        get_cf_outs_as_env(stack_names=stack_names, out_file=out_file)
    get_secrets_as_env(secret_id=secret_id, out_file=out_file)


if __name__ == "__main__":
    parser = ArgumentParser(
        prog="Generate_env_file",
        description="Generate dot env file for deployment",
        epilog="Contact Marouane for extra help",
    )
    parser.add_argument(
        "--secret-id",
        dest="secret_id",
        help="AWS secret id",
        required=True,
    )
    parser.add_argument(
        "--stack-names",
        dest="stack_names",
        help="Cloudformation Stack names (comma separated). If the flag is used without a value, or if the flag is omitted entirely, it defaults to None.",
        required=False,
        default=None,
        nargs='?',
        const=None,
    )
    parser.add_argument(
        "--env-file",
        dest="env_file",
        help=".env file to write to",
        required=False,
        default=".env",
    )

    args = parser.parse_args()

    secret_id, stack_names, env_file = (
        args.secret_id,
        args.stack_names,
        args.env_file
    )



    generate_env_file(
        stack_names=stack_names,
        secret_id=secret_id,
        out_file=env_file
    )
