import yaml
from pathlib import Path
import json
import argparse

def parse_public_functions(fp: Path):
    """Parse the public functions from a yaml file, and print them as a json list."""
    file_text = fp.read_text()

    public_functions_dict = yaml.safe_load(file_text)

    json_str = json.dumps(public_functions_dict, indent=4)
    print(json_str)


if __name__ == '__main__':
    args = argparse.ArgumentParser()

    args.add_argument(
        'yaml_file',
        help='The yaml file to parse.',
    )

    args = args.parse_args()

    fp = Path(args.yaml_file)
    parse_public_functions(fp)
