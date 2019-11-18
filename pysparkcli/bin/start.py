#!/usr/bin/python
import sys
import click
import os

from pathlib import Path

@click.group()
def start():
	BASE_PATH = Path(__file__)
	PROJECT_TEMPLATE_PATH = BASE_PATH.resolve(strict=True).parents[0] / "project-template" / "project_name"
	print(PROJECT_TEMPLATE_PATH)
	print(BASE_PATH)

	print(sys.argv)
	print("?"*40)
	print(PROJECT_TEMPLATE_PATH.resolve())

@start.command(help="Create Project")
@click.option("--master", help="Enter master URL", required=True)
@click.option("--name", help="Enter Project Name", required=True)
@click.option("--cores", help="Enter number of core", type=click.INT)
def create(master, name, cores):
	click.echo("Master: {}".format(master))
	click.echo("\nName: {}".format(name))
	click.echo("\nCores: {}".format(cores))

	proj_path =	Path(name)
	proj_path.mkdir()


if __name__ == "__main__":
	start()
