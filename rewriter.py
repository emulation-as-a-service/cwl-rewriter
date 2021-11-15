# Imports
import json
import os.path
import shutil
import tarfile
import argparse
import time
from pathlib import Path, PurePath
from threading import Thread

import requests
from cwl_utils.parser_v1_0 import CommandInputArraySchema, Dirent, InitialWorkDirRequirement, \
    DockerRequirement, CommandLineTool, Workflow, CommandLineBinding, CommandOutputParameter, File, \
    CommandOutputBinding
from git import Repo
from ruamel import yaml
import sys

# File Input - This is the only thing you will need to adjust or take in as an input to your function:
from ruamel.yaml import StringIO
from ruamel.yaml.scalarstring import DoubleQuotedScalarString

import containerImport


def convert_tool_to_yaml(tool):
    tool_dict = tool.save(top=True)

    # FIXME manually overwriting cwl_obj.id messes up everything else, so right now I just delete it
    tool_dict.pop("id")

    io = StringIO()

    yaml.scalarstring.walk_tree(tool_dict)

    yaml.round_trip_dump(tool_dict, io, default_style=None, default_flow_style=False, indent=2,
                         block_seq_indent=0, line_break=0, explicit_start=False)

    # print(io.getvalue())

    return io.getvalue()


def rewrite(cwl_file, should_upload=False):
    # Read in the cwl file from a yaml
    with open(cwl_file, "r") as cwl_h:
        yaml_obj = yaml.main.round_trip_load(cwl_h, preserve_quotes=True)
        # str_obj = cwl_file.read_text()

    # Check CWLVersion
    if 'cwlVersion' not in list(yaml_obj.keys()):
        print("Error - could not get the cwlVersion")
        sys.exit(1)

    # Import parser based on CWL Version
    if yaml_obj['cwlVersion'] == 'v1.0':
        from cwl_utils import parser_v1_0 as parser
    elif yaml_obj['cwlVersion'] == 'v1.1':
        from cwl_utils import parser_v1_1 as parser
    elif yaml_obj['cwlVersion'] == 'v1.2':
        from cwl_utils import parser_v1_2 as parser
    else:
        print("Version error. Did not recognise {} as a CWL version".format(yaml_obj["CWLVersion"]))
        sys.exit(1)

    # Import CWL Object
    cwl_file = cwl_file.resolve()

    print(cwl_file.as_uri())

    cwl_obj = parser.load_document_by_yaml(yaml_obj, cwl_file.as_uri())

    # cwl_obj = parser.load_document_by_string(str_obj, "") #Path("").as_uri())# cwl_file.as_uri())

    # print("List of object attributes:\n{}".format("\n".join(map(str, dir(cwl_obj)))))

    # TODO add flag to skip command line tool if no dockerPull
    if isinstance(cwl_obj, Workflow):

        # This is necessary as the CWL parser appends an additional "/" in front of windows paths...
        # pathlib does not seem to work properly here
        cut_path_hack = 7 if os.name == "posix" else 8

        if cwl_obj.steps:
            print("WORKFLOW DETECTED ")

            # FIXME check if [8:] works on unix as well
            # TODO only rewrite if dockerPull flag in command line tool
            for step in cwl_obj.steps:
                print("Type step:", type(step.run))

                print("Cutting from:", cut_path_hack)

                print("Step:", step.run, step.run[cut_path_hack:])
                print(step.id)
                is_rewritten = rewrite(Path(step.run[cut_path_hack:]), should_upload)

                if not is_rewritten:
                    continue

                head, tail = os.path.split(
                    Path(step.run[cut_path_hack:]))  # use this for the actual file?
                rewritten_name = head + "/wrapped_" + tail
                rewritten_name = rewritten_name.replace("\\", "/")

                print("Rewritten name:", rewritten_name)
                print("CWL FILE:", cwl_file)
                print("REL:", os.path.relpath(rewritten_name, os.path.dirname(cwl_file)))
                step.run = os.path.relpath(rewritten_name, os.path.dirname(cwl_file))

            # cwl_obj.id = cwl_obj.id[8:]
            print("ID::", cwl_obj.id)
            head_wf, tail_wf = os.path.split(
                cwl_file.as_uri()[cut_path_hack:])  # use this for the actual file?
            uri_ = head_wf + "/wrapped_workflow_" + tail_wf
            print("Writing file to:", uri_)
            with open(uri_, "w+") as f:
                final = convert_tool_to_yaml(cwl_obj)
                f.write(final)

            return

    docker_pull = None
    docker_output_directory = ""
    original_initial_workdir_req_listing = []

    if cwl_obj.hints:

        has_docker_hint = False
        for hint in cwl_obj.hints:
            if hint["class"] == "DockerRequirement":
                has_docker_hint = True
                docker_pull = hint["dockerPull"]
                if "dockerOutputDirectory" in hint:
                    docker_output_directory = hint["dockerOutputDirectory"]
            break
        if has_docker_hint:
            cwl_obj.hints.remove(hint)

    if cwl_obj.requirements:
        docker_req_found = False
        for req in cwl_obj.requirements:
            if type(req) == DockerRequirement:
                docker_req_found = True
                if req.dockerPull:
                    docker_pull = req.dockerPull
                    req.dockerPull = "aeolic/cwl-wrapper:2.7.9"
                if req.dockerOutputDirectory:
                    docker_output_directory = req.dockerOutputDirectory
                req.dockerOutputDirectory = "/app/output"

            if type(req) == InitialWorkDirRequirement:
                original_initial_workdir_req_listing.extend(req.listing)

            # TODO other requirements + hints (interesting for prov doc)

        if not docker_req_found:
            docker_req = DockerRequirement(dockerPull="aeolic/cwl-wrapper:2.7.9",
                                           dockerOutputDirectory="/app/output")  # TODO remove duplicate
            cwl_obj.requirements.append(docker_req)

    # except Exception as e:
    #     print("Something went wrong while reading DockerRequirement:", e)

    if not docker_pull:
        print("CWL did not have dockerPull, returning")
        return False

    env_id = "50e4bdfa-0762-430e-abae-7b73c4b50da4"

    if should_upload:
        env_id = containerImport.import_image(docker_pull)

    output_folder = docker_output_directory if docker_output_directory else "/output"

    config_json = {
        "environmentId": env_id,
        "outputFolder": output_folder,
        "initialWorkDirRequirements": [x.entryname for x in original_initial_workdir_req_listing]
    }

    for inp in cwl_obj.inputs:
        inp.id = inp.id.split("#")[1]  # to remove absolut paths

    for outp in cwl_obj.outputs:
        outp.id = outp.id.split("#")[1]

    # outpBinding = CommandOutputBinding(glob="*.log")
    # log_output = CommandOutputParameter("logfile", type="File", outputBinding=outpBinding)
    # cwl_obj.outputs.append(log_output)

    entry = json.dumps(config_json, indent=4, separators=(',', ': '))

    new_workdir_req = [Dirent(entry, entryname="config.json")]  # TODO to list: []?

    # TODO ADD WRAPPER AS BASE COMMAND

    if not isinstance(cwl_obj.baseCommand, list):
        cwl_obj.baseCommand = [cwl_obj.baseCommand]

    cwl_obj.baseCommand.insert(0, "python3")
    cwl_obj.baseCommand.insert(1, "/app/wrapper.py")

    config_json_set = False
    if not cwl_obj.requirements:
        cwl_obj.requirements = []
    for req in cwl_obj.requirements:
        if type(req) == InitialWorkDirRequirement:
            req.listing.append(new_workdir_req)
            config_json_set = True

    if not config_json_set:
        config_json_req = InitialWorkDirRequirement(new_workdir_req)

        cwl_obj.requirements.append(config_json_req)

    head, tail = os.path.split(cwl_file)  # use this for the actual file?
    rewritten_name = head + "/wrapped_" + tail

    print("----- OUTPUT: Writing file to:", rewritten_name)
    with open(rewritten_name, "w+") as f:
        final = convert_tool_to_yaml(cwl_obj)

        # final = final.replace('"${var', '${var')
        # final = final.replace('r}"', 'r}')

        f.write(final)

    # TODO output in general (change backend!)
    # TODO when using runtime.outdir, wrapper output will be used instead of proper output path

    return True


def onerror(func, path, exc_info):
    """
    Error handler for ``shutil.rmtree``.

    If the error is due to an access error (read only file)
    it attempts to add write permission and then retries.

    If the error is for another reason it re-raises the error.

    Usage : ``shutil.rmtree(path, onerror=onerror)``
    """
    import stat
    # Is the error an access error?
    if not os.access(path, os.W_OK):
        os.chmod(path, stat.S_IWUSR)
        func(path)
    else:
        raise


def clone_repo(git_url):
    # e.g. https://github.com/Aeolic/example-workflow/blob/main/example_workflow.cwl

    if Path.exists(Path("git_repo")):
        shutil.rmtree("git_repo", onerror=onerror)
    baseurl, file_path = git_url.split("/blob/main/")
    print(baseurl, file_path)
    absolute_repo_path = Path("git_repo").absolute()
    print("ABS:", absolute_repo_path)

    file_to_rewrite = os.path.join(absolute_repo_path, file_path)
    print(file_to_rewrite)

    Repo.clone_from(baseurl, "git_repo")
    return file_to_rewrite


def tar_rewritten(output):
    with tarfile.open(output, "w:gz") as tar:
        tar.add("git_repo")


def rewrite_from_repo(git_url, should_upload, output):
    file_to_rewrite = clone_repo(git_url)
    rewrite(Path(file_to_rewrite), should_upload)
    tar_rewritten(output)


# rewrite_from_repo("https://github.com/Aeolic/example-workflow/blob/main/example_workflow.cwl")

if __name__ == '__main__':

    my_parser = argparse.ArgumentParser(
        description="CWL Rewriter. Rewrites CWL Workflows to use the EaaS-Framework instead of docker containers.")
    my_parser.add_argument("url_or_path", nargs="?")
    my_parser.add_argument("--repo", dest="repo", action="store_true")
    my_parser.add_argument("--no-upload", dest="upload", action="store_false",
                           help="Will use a placeholder ID instead of uploading the container and using the return id."
                                " !!! CWL will only work after manually putting in the proper IDs, when this option is used!!!")
    my_parser.add_argument("-o", dest="output",
                           help="The path where the rewritten repository is stored. (Only works with --repo)",
                           default="rewritten.tgz")
    my_parser.set_defaults(repo=False)
    my_parser.set_defaults(upload=True)

    args = my_parser.parse_args()
    print("ARGS:", args)

    if args.repo:
        rewrite_from_repo(args.url_or_path, args.upload, args.output)
    else:
        rewrite(Path(args.url_or_path), args.upload)

# TODO:
# runtime.X
# env requirement
# cleanup!
