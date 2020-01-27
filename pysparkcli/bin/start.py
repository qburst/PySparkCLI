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
	click.echo("Using CLI Version: {}".format(__version__))
	BASE_PATH = Path(__file__)
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
@click.option('--packages', '-p', type=click.STRING)
@click.option('--class_name', '-c', type=click.STRING)
@click.option('--jars', '-j', type=click.STRING)
@click.option('--py_files', '-f', type=click.STRING)
def run(project, packages, class_name, jars, py_files):
	""" Run Project: \n
		Example:
		pysparkcli run 'testProject'"""
	click.echo("Using CLI Version: {}".format(__version__))
	click.echo("Started running project: {}".format(project))
	if Path("{}/requirements.txt".format(project)).exists():
		os.system("pip install -r {}/requirements.txt".format(project))
	submit_command = "spark-submit {prj}/src/app.py --name {prj}".format(prj=project)
	if py_files:
		submit_command += " --py_files {}".format(py_files)
	if packages:
		submit_command += " --packages {}".format(packages)
	if jars:
		submit_command += " --jars {}".format(jars)
	if packages:
		submit_command += " --class {}".format(class_name)
	os.system(submit_command)
	print("Completed running {}!".format(project))

@start.command()
@click.argument('project', type=click.STRING, required=True)
@click.argument('path', type=click.STRING, required=True)
def stream(project, path):
	""" Start Data Stream: \n
		Example:
		pysparkcli stream 'testProject' 'twitter_stream'"""
	click.echo("Using CLI Version: {}".format(__version__))
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
	click.echo("Using CLI Version: {}".format(__version__))
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
	click.echo("Using CLI Version: {}".format(__version__))
	# Start the execution of command here
	start()
