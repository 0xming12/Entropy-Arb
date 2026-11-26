"""AWS CLI wrappers for the controller's authenticated SSM session."""

import json
import os
import subprocess


def aws_command():
    args = [os.environ.get("AWS_CLI", "aws"), "--no-cli-pager"]
    if profile := os.environ.get("AWS_PROFILE"):
        args += ["--profile", profile]
    if region := os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"):
        args += ["--region", region]
    return args


def instance():
    """Read the authorized target from the controller's environment."""
    value = os.environ.get("ENTROPY_NVDA_INSTANCE", "")
    if not value.startswith("i-") or len(value) not in (10, 19):
        raise ValueError("set ENTROPY_NVDA_INSTANCE to an EC2 instance ID")
    if any(ch not in "0123456789abcdef" for ch in value[2:]):
        raise ValueError("invalid EC2 instance ID")
    return value


def cli(*args):
    return subprocess.check_output([*aws_command(), "ssm", *args], text=True).strip()


def invocation(command_id):
    return json.loads(cli("get-command-invocation", "--instance-id", instance(),
                          "--command-id", command_id, "--output", "json"))
