#!/usr/bin/python
import sys
import click
import os


from pathlib import Path
from pysparkcli.core.admin import TemplateParser

@click.group()
def start():
	pass

@start.command()
@click.option("--master", "-m", help="Enter master URL")
@click.option("--cores", "-c", help="Enter number of core", type=click.INT)
@click.argument('project', type=click.STRING, required=True)
def create(master, cores, project):
	""" Create Project: \n
		Example:
		pysparkcli create 'testProject' -m 'local'"""
	BASE_PATH = Path(__file__)
	PROJECT_TEMPLATE_PATH = BASE_PATH.resolve().parents[1] / "project-template" / "project_name"
	
	context = {
		"sample": project,
		"master_url": master if master else 'local[*]',
		"cores": cores if cores else 2,
		"docs_version": "1.0.0"
	}

	# add project modules to sys.path
	PATH = Path.cwd() / (project + "/src")
	sys.path.append(str(PATH.as_posix()))

	# build the new project folder from template
	TemplateParser().build_project(PROJECT_TEMPLATE_PATH, context, project)
	click.echo("Completed building project: {}".format(project))

@start.command()
@click.argument('project', type=click.STRING, required=True)
def run(project):
	""" Run Project: \n
		Example:
		pysparkcli run 'testProject'"""
	click.echo("Started running project: {}".format(project))
	os.system("spark-submit {}/src/app.py".format(project))

@start.command(help="Run Test")
@click.argument('project', type=click.STRING, required=True)
def test(project):
	click.echo("Started running project: {}".format(project))
	os.system("spark-submit {}/src/app.py".format(project))

if __name__ == "__main__":
	# Start the execution of command here
	start()
