import time
import requests

EMIL_BASE_URL = "https://historic-builds.emulation.cloud/emil"


def poll_until_done(task_id):
    while True:
        task_response = requests.get(EMIL_BASE_URL + "/tasks/" + task_id)
        as_json = task_response.json()
        print(as_json)
        task_id = as_json["taskId"]
        is_done = as_json["isDone"]

        if is_done:
            return as_json
        else:
            time.sleep(5)
            # print("Task", task_id, "is not finished yet.")


def import_image(dockerPull, runtime_id):
    print("Starting container Import for", dockerPull)
    if ":" in dockerPull:
        container, tag = dockerPull.split(":")
    else:
        container = dockerPull
        tag = "latest"
    print("Got container:", container, "tag:", tag)
    json_container_request = {"containerType": "dockerhub",
                              "urlString": container,
                              "tag": tag}

    print("Sending request to build image with data:", json_container_request)
    task_response = requests.post(EMIL_BASE_URL + "/EmilContainerData/buildContainerImage",
                                  json=json_container_request)

    image_task_id = task_response.json()["taskId"]

    # TODO error handling everywhere
    image_done = poll_until_done(image_task_id)
    response_obj = image_done["object"]
    print("Got response:", response_obj)

    if response_obj:
        data = eval(response_obj)
        meta = data["metadata"]

        print("Evaluating if data was successful:", data)
        import_data = {
            "imageUrl": data["containerUrl"],
            "processArgs": meta.get("entryProcesses", []),
            "processEnvs": meta.get("envVariables", []),
            "workingDir": meta.get("workingDir", "/"),
            "name": "CWL_auto_import_" + container + ":" + tag,
            "inputFolder": "/input",  # irrelevant, gets overwritten by execution anyway
            "outputFolder": "/app/output",
            # irrelevant, gets overwritten by execution anyway (TODO maybe set properly anyway)
            "imageType": "dockerhub",
            "title": "CWL_auto_import_" + container + ":" + tag,
            "description": '<p>Automatic import by CWL Rewriter </p>',
            # TODO use CWL as description?
            "author": "CWL Rewriter",  # TODO check CWL for author
            "runtimeId": runtime_id,
            "serviceContainer": False,
            "enableNetwork": False,
            "archive": "default",
            "containerDigest": meta.get("containerDigest", ""),
            "containerSourceUrl": meta.get("containerSourceUrl", ""),
            "tag": meta.get("tag", "")
        }

        print("Sending import Request with data:", import_data)
        import_response = requests.post(EMIL_BASE_URL + "/EmilContainerData/importContainer",
                                        json=import_data)

        import_task_id = import_response.json()["taskId"]

        import_done = poll_until_done(import_task_id)
        env_id = import_done["userData"]["environmentId"]
        print("Got env Id:", env_id)
        return env_id


if __name__ == '__main__':
    import_image("frolvlad/alpine-bash:latest", "ea52aafa-56e7-479d-b756-ba82a1328b2f")
