#!/usr/bin/python
import click
import os


from pathlib import Path
from pysparkcli.core.admin import TemplateParser
from pysparkcli import __version__

@click.group()
def start():
	pass

@start.command()
@click.option("--master", "-m", help="Enter master URL")
@click.option("--project_type", "-t", help="Enter type of project", type=click.Choice(['default', 'streaming'], case_sensitive=False))
@click.option("--cores", "-c", help="Enter number of core", type=click.INT)
@click.argument('project', type=click.STRING, required=True)
def create(master, cores, project, project_type):
	""" Create Project: \n
		Example:
		pysparkcli create 'testProject' -m 'local'"""
	BASE_PATH = Path(__file__)
	print(project_type)
	if project_type:
		PROJECT_TEMPLATE_PATH = BASE_PATH.resolve().parents[1] / "project-template" / project_type / "project_name"
	else:
		PROJECT_TEMPLATE_PATH = BASE_PATH.resolve().parents[1] / "project-template" / "default" / "project_name"

	context = {
		"project_name": project,
		"master_url": master if master else 'local[*]',
		"cores": cores if cores else 2,
		"docs_version": __version__
	}
	
	# build the new project folder from template
	TemplateParser().build_project(PROJECT_TEMPLATE_PATH, context, project)
	click.echo("Completed building project: {}".format(project))

@start.command()
@click.argument('project', type=click.STRING, required=True)
@click.option('--path', '-p', type=click.STRING)
def run(project, path):
	""" Run Project: \n
		Example:
		pysparkcli run 'testProject'"""
	click.echo("Started running project: {}".format(project))
	if Path("{}/requirements.txt".format(project)).exists():
		os.system("pip install -r {}/requirements.txt".format(project))
	os.system("python {prj}/src/app.py".format(prj = project, path = path))
	os.system("spark-submit {}/src/app.py".format(project))

@start.command()
@click.argument('project', type=click.STRING, required=True)
@click.argument('path', type=click.STRING, required=True)
def stream(project, path):
	""" Start Data Stream: \n
		Example:
		pysparkcli stream 'testProject' 'twitter_stream'"""
	click.echo("Started streaming of project: {}".format(project))
	if Path("{}/requirements.txt".format(project)).exists():
		os.system("pip install -r {}/requirements.txt".format(project))
	os.system("python {}/src/streaming/{}.py".format(project, path))

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
