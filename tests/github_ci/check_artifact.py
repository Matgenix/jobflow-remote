import json
import sys
import uuid
from pathlib import Path

# Get the directory where the artifact has been unzipped
if len(sys.argv) != 2:
    print("Usage: python check_artifact.py <unzipped_artifact_directory>")
    sys.exit(1)
artifact_directory = sys.argv[1]


def load_json(filename):
    """Load json file."""
    with open(filename) as f:
        return json.load(f)


def is_uuid(string):
    """Check if string is a valid uuid."""
    if not isinstance(string, str):
        return False
    try:
        uuid.UUID(string)
    except ValueError:
        return False
    return True


def get_tree_dict(path):
    """Recursively a dict representation of the directory tree."""
    path = Path(path)
    tree = {}
    for item in path.iterdir():  # No sorting applied
        if item.is_dir():
            tree[item.name] = get_tree_dict(item)  # Recursive call for subdirectories
        else:
            content = load_json(item)
            if item.name == "test_info.json":
                tree[item.name] = content
            elif item.name == "flows.json":
                tree[item.name] = [
                    {
                        "name": fd.get("name"),
                        "state": fd.get("state"),
                        "_id_class": fd.get("_id").get("@class")
                        if fd.get("_id") is not None
                        else None,
                    }
                    for fd in sorted(content, key=lambda x: x["name"])
                ]
            elif item.name == "jf_auxiliary.json":
                tree[item.name] = {
                    "ndocs": len(content),
                    "running_runner": len(
                        [doc for doc in content if "running_runner" in doc]
                    ),
                    "environment_info": len(
                        [doc for doc in content if "full_environment" in doc]
                    ),
                    "next_id": len([doc for doc in content if "next_id" in doc]),
                }
            elif item.name == "jobs.json":
                tree[item.name] = [
                    {
                        "state": jd.get("state"),
                        "worker": jd.get("worker"),
                        "has_uuid": "uuid" in jd,
                        "uuid_is_uuid": is_uuid(jd.get("uuid")),
                        "_id_class": jd.get("_id").get("@class")
                        if jd.get("_id") is not None
                        else None,
                        "job_name": jd.get("job").get("name")
                        if jd.get("job") is not None
                        else None,
                    }
                    for jd in sorted(content, key=lambda x: x["db_id"])
                ]
            else:
                raise RuntimeError(f"Should not have a file named '{item.name}'")
    return tree


def save_tree_as_json(directory, output_file):
    """Generates and saves the directory tree as a JSON file."""
    tree_dict = get_tree_dict(directory)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(tree_dict, f, indent=4, ensure_ascii=False)  # Pretty-print JSON


# Get the reference tree directory
this_dir = Path(__file__).parent.resolve()
with open(this_dir / "ref_data" / "ref_artifact_directory_tree.json") as f:
    ref_artifact_directory_tree = json.load(f)


# Get the tree directory of the downloaded artifact
artifact_directory_tree = get_tree_dict(artifact_directory)
if list(artifact_directory_tree.keys()) != ["github_ci"]:
    print("Uploaded artifact different from reference.")
    sys.exit()


if artifact_directory_tree["github_ci"] != ref_artifact_directory_tree:
    import pprint

    print("Uploaded artifact different from reference.")
    print("Reference:")
    pprint.pprint(ref_artifact_directory_tree, indent=2)
    print("Uploaded artifact:")
    pprint.pprint(artifact_directory_tree["github_ci"], indent=2)
    sys.exit(1)
