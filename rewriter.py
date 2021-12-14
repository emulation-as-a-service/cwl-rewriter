import json
import os.path
import shutil
import tarfile
import argparse
import sys

from pathlib import Path

from cwl_utils.parser import load_document_by_yaml
from cwl_utils.parser.cwl_v1_0 import InitialWorkDirRequirement, EnvVarRequirement, Dirent, \
    Workflow, DockerRequirement
from git import Repo
from ruamel import yaml

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


def rewrite(cwl_file, should_upload=False, runtime_id=""):
    # Read in the cwl file from a yaml
    with open(cwl_file, "r") as cwl_h:
        yaml_obj = yaml.main.round_trip_load(cwl_h, preserve_quotes=True)

    # Check CWLVersion
    if 'cwlVersion' not in list(yaml_obj.keys()):
        print("Error - could not get the cwlVersion")
        sys.exit(1)

    # Import CWL Object
    cwl_file = cwl_file.resolve()
    cwl_obj = load_document_by_yaml(yaml_obj, cwl_file.as_uri())

    # if the parsed object is a Workflow, rewrite all CommandLineTools that are part of it

    #TODO when executed on windows, \ will be used thus rendering the workflow useless for linux
    if isinstance(cwl_obj, Workflow):

        # This is necessary as the CWL parser appends an additional "/" in front of windows paths...
        # pathlib does not seem to work properly here
        cut_path_hack = 7 if os.name == "posix" else 8

        if cwl_obj.steps:
            print("Workflow detected for", cwl_file.as_uri())

            for step in cwl_obj.steps:
                print("Step:", step.run, step.run[cut_path_hack:])
                is_rewritten = rewrite(Path(step.run[cut_path_hack:]), should_upload)

                # skip CWL files without DockerRequirement
                if not is_rewritten:
                    continue

                head, tail = os.path.split(
                    Path(step.run[cut_path_hack:]))  # use this for the actual file?
                rewritten_name = head + "/wrapped_" + tail

                # print("Rewritten name:", rewritten_name)
                # print("CWL FILE:", cwl_file)
                # print("REL:", os.path.relpath(rewritten_name, os.path.dirname(cwl_file)))
                step.run = os.path.relpath(rewritten_name, os.path.dirname(cwl_file)).replace("\\", "/")

            head_wf, tail_wf = os.path.split(
                cwl_file.as_uri()[cut_path_hack:])  # use this for the actual file?
            uri_ = head_wf + "/wrapped_workflow_" + tail_wf
            print("Storing Rewritten CWL Workflow as:", uri_)
            with open(uri_, "w+") as f:
                final = convert_tool_to_yaml(cwl_obj)
                f.write(final)
            return

    # --- CommandLineTool
    # if this is reached, the CWL file is a CommandLineTool and will be rewritten accordingly

    docker_pull = None
    docker_output_directory = ""
    original_initial_workdir_req_listing = []
    env_var_requirements = {}

    # TODO General solution for all hints/Requirements
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
            cwl_obj.hints.remove(hint)  # move DockerRequirement to Requirements

    wrapper_docker_name = "aeolic/cwl-wrapper:3.0.0"
    if cwl_obj.requirements:
        docker_req_found = False
        for req in cwl_obj.requirements:
            if type(req) == DockerRequirement:
                docker_req_found = True
                if req.dockerPull:
                    docker_pull = req.dockerPull
                    req.dockerPull = wrapper_docker_name
                if req.dockerOutputDirectory:
                    docker_output_directory = req.dockerOutputDirectory
                req.dockerOutputDirectory = "/app/output"

            if type(req) == InitialWorkDirRequirement:
                original_initial_workdir_req_listing.extend(req.listing)

            if type(req) == EnvVarRequirement:
                for env_var in req.envDef:
                    env_var_requirements[env_var.envName] = env_var.envValue
            # TODO other requirements + hints

        if not docker_req_found:
            docker_req = DockerRequirement(dockerPull=wrapper_docker_name,
                                           dockerOutputDirectory="/app/output")
            cwl_obj.requirements.append(docker_req)

    if not docker_pull:
        print(cwl_file.as_uri(), "does not specify dockerPull, not rewriting...")
        return False

    # placeholder id if rewriter is launched with --no-upload
    env_id = "PLACE_HOLDER_ID_NEEDS_TO_BE_SET_MANUALLY"

    if should_upload:
        env_id = containerImport.import_image(docker_pull, runtime_id)

    output_folder = docker_output_directory if docker_output_directory else "/output"

    config_json = {
        "environmentId": env_id,
        "outputFolder": output_folder,
        "initialWorkDirRequirements": [x.entryname for x in original_initial_workdir_req_listing]
    }

    if env_var_requirements:
        config_json["environmentVariables"] = env_var_requirements

    for inp in cwl_obj.inputs:
        inp.id = inp.id.split("#")[1]  # to remove absolute paths

    for outp in cwl_obj.outputs:
        outp.id = outp.id.split("#")[1]  # to remove absolute paths

    entry = json.dumps(config_json, indent=4, separators=(',', ': '))

    new_workdir_req = [Dirent(entry, entryname="config.json")]

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

    print("Storing CommandLineTool at", rewritten_name)
    with open(rewritten_name, "w+") as f:
        final = convert_tool_to_yaml(cwl_obj)
        f.write(final)

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


def rewrite_from_repo(git_url, should_upload, output, runtime_id):
    file_to_rewrite = clone_repo(git_url)
    rewrite(Path(file_to_rewrite), should_upload, runtime_id)
    tar_rewritten(output)


if __name__ == '__main__':

    my_parser = argparse.ArgumentParser(
        description="CWL Rewriter. Rewrites CWL Workflows to use the EaaS-Framework instead of docker containers.")
    my_parser.add_argument("url_or_path", nargs="?")
    my_parser.add_argument("--repo", dest="repo", action="store_true")
    my_parser.add_argument("--no-upload", dest="upload", action="store_false",
                           help="Will use a placeholder ID instead of uploading the container and using the return id."
                                " !!! CWL will only work after manually putting in the proper IDs, when this option is used!!!")
    my_parser.add_argument("-o", dest="output",
                           help="The path where the rewritten repository will be stored. (Only works with --repo)",
                           default="rewritten.tgz")
    my_parser.add_argument("--runtime-id", dest="runtime_id",
                           help="The UUID of the container runtime.", default=None)
    my_parser.set_defaults(repo=False)
    my_parser.set_defaults(upload=True)

    args = my_parser.parse_args()
    print("Rewriter called with:", args)

    if args.upload:
        if not args.runtime_id:
            print("You need to supply a runtime ID unless using --no-upload.")
            exit(1)

    if args.repo:
        rewrite_from_repo(args.url_or_path, args.upload, args.output, args.runtime_id)
    else:
        rewrite(Path(args.url_or_path), args.upload, args.runtime_id)
