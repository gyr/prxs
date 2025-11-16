from rich.console import Console
from rich.rule import Rule
from rich.table import Table
from typing import Generator, Dict, Any
from relx.providers import DataSourcer

from relx.utils.logger import logger_setup
from relx.utils.tools import running_spinner_decorator


log = logger_setup(__name__)


@running_spinner_decorator
def get_groups(
    data_sourcer: DataSourcer, api_url: str, group: str, is_fulllist: bool = False
) -> Dict[str, Any]:
    """
    Given a group name return the OBS info about it."

    :param api_url: OBS instance
    :param group: OBS group name
    :return: OBS group info
    """
    info = data_sourcer.get_groups(api_url, group, is_fulllist)
    if not info: # Assuming an empty dict means not found
        raise RuntimeError(f"{group} not found.")
    return info


@running_spinner_decorator
def get_users(
    data_sourcer: DataSourcer,
    api_url: str,
    search_text: str,
    is_login: bool = True,
    is_email: bool = False,
    is_realname: bool = False,
) -> Generator:
    """
    Given a source package return the OBS user of the bugowner"

    :param api_url: OBS instance
    :param search_text: Text to be search OBS project for user info
    :param is_login: Search based on user login
    :param is_email: Search based on user email
    :param is_realname: Search based on user name
    :return: OBS user info
    """
    users_info = data_sourcer.get_users(
        api_url, search_text, is_login, is_email, is_realname
    )
    if not users_info:
        raise RuntimeError(f"{search_text} not found.")
    for info in users_info:
        yield info


def build_parser(parent_parser, config) -> None:
    """
    Builds the parser for this script. This is executed by the main CLI
    dynamically.

    :param config: Lua config table
    :return: The subparsers object from argparse.
    """
    subparser = parent_parser.add_parser(
        "users",
        help="Search in OBS information for the given user/group.",
    )
    # Mutually exclusive group within the subparser
    group = subparser.add_mutually_exclusive_group(required=True)
    group.add_argument("--group", "-g", action="store_true", help="Search for group.")
    group.add_argument(
        "--login", "-l", action="store_true", help="Search user for login."
    )
    group.add_argument(
        "--email", "-e", action="store_true", help="Search user for email."
    )
    group.add_argument(
        "--name", "-n", action="store_true", help="Search user for name."
    )
    subparser.add_argument("search_text", type=str, help="Search text.")
    subparser.set_defaults(func=main)


def main(args, config, data_sourcer: DataSourcer) -> None:
    """
    Main method that get the OBS user from the bugowner for the given binary package.

    :param args: Argparse Namespace that has all the arguments
    :param config: Lua config table
    """
    console = Console()
    try:
        table = Table(show_header=False)
        if args.group:
            group_info = get_groups(
                data_sourcer, args.osc_instance, args.search_text, True
            )
            for key, value in group_info.items():
                log.debug("%s: %s", key, value)
                table.add_row(key, str(value))
        else:
            for info in get_users(
                data_sourcer,
                args.osc_instance,
                args.search_text,
                args.login,
                args.email,
                args.name,
            ):
                for key, value in info.items():
                    log.debug("%s: %s", key, value)
                    table.add_row(key, str(value))
                table.add_row(Rule(style="dim"), Rule(style="dim"))
        console.print(table)
    except RuntimeError as e:
        log.error(e)
