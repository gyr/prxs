"""
This module provides the data sourcer and its providers.
"""

import shlex
from typing import List, Dict, Any
from lxml import etree
from subprocess import CalledProcessError

from relx.utils.tools import run_command


class OscProvider:
    """
    Provides data from the osc command.
    """

    def list_packages(self, api_url: str, project: str) -> List[str]:
        """
        List all source packages from a OBS project.
        """
        command = f"osc -A {api_url} ls {project}"
        result = run_command(shlex.split(command))
        if result.stdout:
            return [line.strip() for line in result.stdout.strip().splitlines()]
        return []

    def list_artifacts(
        self,
        api_url: str,
        project: str,
        package: str,
        repo_name: str,
        invalid_start: List[str],
        invalid_extensions: List[str],
    ) -> List[str]:
        """
        List all artifacts for a given package and repository.
        """
        command = f"osc -A {api_url} ls {project} {package} -b -r {repo_name}"
        result = run_command(shlex.split(command))
        if not result.stdout:
            return []

        filtered_artifacts = []
        for line in result.stdout.strip().splitlines():
            line = line.strip()
            if not line.startswith(tuple(invalid_start)) and not line.startswith(
                f"{repo_name}/"
            ):
                if not line.endswith(tuple(invalid_extensions)):
                    filtered_artifacts.append(line)
        return filtered_artifacts

    def get_groups(self, api_url: str, group: str, is_fulllist: bool = False) -> Dict[str, Any]:
        """
        Get information about a group.
        """
        try:
            command = f'osc -A {api_url} api "/group/{group}"'
            output = run_command(shlex.split(command))
            tree = etree.fromstring(output.stdout.encode())
            info = {}

            title = tree.find("title")
            info["Group"] = title.text if title is not None else None

            email = tree.find("email")
            info["Email"] = email.text if email is not None else None

            maintainers = tree.findall("maintainer")
            info["Maintainers"] = [tag.get("userid") for tag in maintainers]

            if is_fulllist:
                people = tree.findall("person")
                users = []
                for person in people:
                    for user in person.findall("person"):
                        users.append(user.get("userid"))
                info["Users"] = users

            return info
        except CalledProcessError as e:
            raise RuntimeError(f"Failed to get group '{group}': {e.stderr.strip()}") from e

    def get_users(
        self,
        api_url: str,
        search_text: str,
        is_login: bool = True,
        is_email: bool = False,
        is_realname: bool = False,
    ) -> List[Dict[str, str]]:
        """
        Get information about users.
        """
        try:
            if is_login:
                command = (
                    f'osc -A {api_url} api \'/search/person?match=@login="{search_text}"\''
                )
            elif is_email:
                command = (
                    f'osc -A {api_url} api \'/search/person?match=@email="{search_text}"\''
                )
            elif is_realname:
                command = f'osc -A {api_url} api \'/search/person?match=contains(@realname,"{search_text}")\''
            else:
                raise RuntimeError("Invalid user search.")
            output = run_command(shlex.split(command))
            tree = etree.fromstring(output.stdout.encode())
            people = tree.findall("person")
            if not people:
                return []
            
            users_info = []
            for person in people:
                info = {
                    "User": person.find("login").text,
                    "Email": person.find("email").text,
                    "Name": person.find("realname").text,
                    "State": person.find("state").text,
                }
                users_info.append(info)
            return users_info
        except CalledProcessError as e:
            raise RuntimeError(f"Failed to get user '{search_text}': {e.stderr.strip()}") from e

class DataSourcer:
    """
    Aggregates data from various providers.
    """

    def __init__(self, osc_provider: OscProvider):
        self._osc_provider = osc_provider

    def list_packages(self, api_url: str, project: str) -> List[str]:
        return self._osc_provider.list_packages(api_url, project)

    def list_artifacts(
        self,
        api_url: str,
        project: str,
        package: str,
        repo_name: str,
        invalid_start: List[str],
        invalid_extensions: List[str],
    ) -> List[str]:
        return self._osc_provider.list_artifacts(
            api_url, project, package, repo_name, invalid_start, invalid_extensions
        )

    def get_groups(self, api_url: str, group: str, is_fulllist: bool = False) -> Dict[str, Any]:
        return self._osc_provider.get_groups(api_url, group, is_fulllist)

    def get_users(
        self,
        api_url: str,
        search_text: str,
        is_login: bool = True,
        is_email: bool = False,
        is_realname: bool = False,
    ) -> List[Dict[str, str]]:
        return self._osc_provider.get_users(
            api_url, search_text, is_login, is_email, is_realname
        )


def create_data_sourcer() -> DataSourcer:
    """
    Factory function to create a DataSourcer instance with all its providers.
    """
    osc_provider = OscProvider()
    return DataSourcer(osc_provider=osc_provider)
