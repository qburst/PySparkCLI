import os, sys
import shutil


from jinja2 import Template
from pathlib import Path

from pyspark.sql import SparkSession


class TemplateParser:
    rewrite_template_suffixes = ['.py-tpl', '.py']
    project_struct = {'files': []}
    tests_path_name = "tests"

    def build_project(self, path, context, name):
        # add project modules to sys.path
        PATH = Path.cwd() / (name + "/src")
        sys.path.insert(0, str(PATH.as_posix()))
        for i in Path(path).iterdir():
            if i.is_dir():
                if i.stem == self.tests_path_name:
                    self.move_to_path(i, name + "/" + self.tests_path_name)
                    continue
                self.project_struct[i.stem] = {'files': []}
                self.handle_directory(i, context, name)
            elif i.is_file():
                self.project_struct['files'].append(i.stem)
                self.build_template(context, i, name)

    def handle_directory(self, directory, context, final_path):
        for i in Path(directory).iterdir():
            if i.is_dir():
                self.handle_directory(i, context, final_path + "/" + directory.stem)
                self.project_struct[directory.stem][i.stem] = {'files': []}
            elif i.is_file():
                if directory.stem not in self.project_struct:
                    self.project_struct[directory.stem] = {'files': []}
                self.project_struct[directory.stem]['files'].append(i.stem)
                self.build_template(context, i, final_path + "/" + directory.stem)

    def build_template(self, context, file, final_path):
        template = Template(file.read_text('utf-8'))
        content = template.render(context)
        new_path = Path.cwd() / final_path
        if not new_path.exists():
            new_path.mkdir(parents=True)
        new_extension = self.rewrite_template_suffixes[1] if file.suffix.endswith("-tpl") else file.suffix
        with open(str(new_path) + "/" + file.stem + new_extension , 'w', encoding='utf-8') as new_file:
            new_file.write(content)
    
    def move_to_path(self, directory, final_path):
        for i in Path(directory).iterdir():
            if i.is_dir():
                self.move_to_path(i, final_path + "/" + i.stem)
            elif i.is_file():
                dest_fpath = final_path + "/" + i.name
                os.makedirs(os.path.dirname(dest_fpath), exist_ok=True)
                shutil.copy(i.as_posix(), dest_fpath)


class SparkBuilder():

    def __init__(self, name):
        self.name = name

    def build_sc(self):
        return SparkSession.builder.master("local").appName("sample").getOrCreate()
