#!/usr/bin/python
import sys
import click
import os


from pathlib import Path
from pysparkcli.core.admin import TemplateParser

@click.group()
def start():
	pass

@start.command(help="Create Project")
@click.option("--master", "-m", help="Enter master URL", required=True)
@click.option("--name", "-n", help="Enter Project Name", required=True)
@click.option("--cores", "-c", help="Enter number of core", type=click.INT)
def create(master, name, cores):

	BASE_PATH = Path(__file__)
	PROJECT_TEMPLATE_PATH = BASE_PATH.resolve(strict=True).parents[1] / "project-template" / "project_name"

	context = {
		"project_name": name,
		"master_url": master,
		"cores": cores,
		"docs_version": "1.0.0"
	}

	# build the new project folder from template
	TemplateParser().build_project(PROJECT_TEMPLATE_PATH, context, name)
	click.echo("Completed building project: {}".format(name))

@start.command(help="Run Project")
@click.option("--name", "-n", help="Enter Project Name", required=True)
def run(name):
	click.echo("Started running project: {}".format(name))
	os.system("spark-submit {}/src/app.py".format(name))

if __name__ == "__main__":
	# Start the execution of command here
	start()
