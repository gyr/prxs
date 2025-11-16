import re
from argparse import Namespace
from rich.progress import Progress, TaskID
from typing import Dict, Any
from relx.providers import DataSourcer

from relx.utils.logger import logger_setup
from relx.utils.tools import running_spinner_decorator


log = logger_setup(__name__)


def list_artifacs(
    api_url: str,
    project: str,
    packages: list[str],
    invalid_start: list[str],
    invalid_extensions: list[str],
    repo_info: Dict[str, str],
    progress: Progress,
    task_id: TaskID,
    data_sourcer: DataSourcer,
) -> None:
    """
    List all artifacts filtered by pattern in the specified repoistory
    from a OBS project

    :param api_url: OBS instance
    :param project: OBS project
    :param project: list of source packages
    :param repo_info: Lua Table with repository info
    """
    log.debug(">> pattern = %s", repo_info["pattern"])
    pattern = re.compile(repo_info["pattern"])
    log.debug(">> pattern = %s", pattern)

    for package in packages:
        if re.search(pattern, package):
            for line in data_sourcer.list_artifacts(
                api_url,
                project,
                package,
                repo_info["name"],
                invalid_start,
                invalid_extensions,
            ):
                print(line)
        progress.update(task_id, advance=1)


def build_parser(parent_parser, config: Dict[str, Any]) -> None:
    """
    Builds the parser for this script. This is executed by the main CLI
    dynamically.

    :param config: Lua config table
    :return: The subparsers object from argparse.
    """
    subparser = parent_parser.add_parser(
        "artifacts", help="Return the list of artifacts from a OBS project."
    )
    subparser.add_argument(
        "--project",
        "-p",
        dest="project",
        help=f"OBS/IBS project (DEFAULT = {config['default_product']}).",
        type=str,
        default=config["default_product"],
    )
    subparser.set_defaults(func=main)


def main(args: Namespace, config: Dict[str, Any], data_sourcer: DataSourcer) -> None:
    """
    Main method that get the list of all artifacts from a given OBS project

    :param args: Argparse Namespace that has all the arguments
    :param config: Lua config table
    """
    # Parse arguments
    parameters = {"api_url": args.osc_instance, "project": args.project}
    packages = data_sourcer.list_packages(**parameters)

    parameters.update(
        {
            "packages": packages,
        }
    )
    total_steps = len(config["artifacts"]["repo_info"]) * len(packages)
    with Progress() as progress:
        task_id = progress.add_task("Searching artifacts", total=total_steps)
        for repo_info in config["artifacts"]["repo_info"]:
            parameters.update(
                {
                    "invalid_start": config["artifacts"]["invalid_start"],
                    "invalid_extensions": config["artifacts"]["invalid_extensions"],
                    "repo_info": repo_info,
                    "progress": progress,
                    "task_id": task_id,
                }
            )
            list_artifacs(**parameters, data_sourcer=data_sourcer)
