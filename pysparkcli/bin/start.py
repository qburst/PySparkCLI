#!/usr/bin/python
import sys
import click

from pathlib import Path
from pysparkcli.core.admin import TemplateParser

@click.group()
def start():
	pass

@start.command(help="Create Project")
@click.option("--master", help="Enter master URL", required=True)
@click.option("--name", help="Enter Project Name", required=True)
@click.option("--cores", help="Enter number of core", type=click.INT)
def create(master, name, cores):
	click.echo("Master: {}".format(master))
	click.echo("\nName: {}".format(name))
	click.echo("\nCores: {}".format(cores))

	BASE_PATH = Path(__file__)
	PROJECT_TEMPLATE_PATH = BASE_PATH.resolve(strict=True).parents[1] / "project-template" / "project_name"
	print(PROJECT_TEMPLATE_PATH)
	print(BASE_PATH)

	print(sys.argv)
	print("?" * 40)
	print(PROJECT_TEMPLATE_PATH.resolve())

	context = {
		"project_name": name,
		"master_url": master,
		"cores": cores,
		"docs_version": "1.0.0"
	}

	# build the new project folder from template
	TemplateParser().build_project(PROJECT_TEMPLATE_PATH, context, name)


if __name__ == "__main__":
	start()
