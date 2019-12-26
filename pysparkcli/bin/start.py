#!/usr/bin/python
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
@click.option("--test", "-t", help="Test case to Run", type=click.STRING)
@click.argument("project", type=click.STRING, required=True)
def test(project, test):
	""" Run Test Cases: \n
		Example:
		pysparkcli test 'testProject'
		pysparkcli test 'testProject' -t etl_job"""
	TESTS_PATH = Path.cwd() / (project + "/tests")
	if test:
		click.echo("Started running test case: {}".format(test))
		os.system("spark-submit {}/tests/test_{}.py".format(project, test))
	else:
		tests = [i for i in os.listdir(TESTS_PATH) if not i.startswith('__init__') and i.endswith(".py")]
		click.echo("Started running test cases for project: {}".format(project))
		for i in tests:
			os.system("spark-submit {}/tests/{}".format(project, i))

if __name__ == "__main__":
	# Start the execution of command here
	start()
