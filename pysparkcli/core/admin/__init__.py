from jinja2 import Template
from pathlib import Path


class TemplateParser:
    rewrite_template_suffixes = ['.py-tpl', '.py']
    project_struct = {'files': []}

    def build_project(self, path, context, name):
        print("/"* 40 + 'Im here'+str(path))
        for i in Path(path).iterdir():
            print(i.stem + "<>"+i.suffix+"///"+str(self.rewrite_template_suffixes))
            print(str(i.is_dir()) + "<>" + str(i.is_file()) + str(i.suffix in self.rewrite_template_suffixes))
            if i.is_dir():
                print("Direvctory")
                self.project_struct[i.stem] = {'files': []}
                self.handle_directory(i, context, name)
            elif i.is_file():
                print("creating " + i.stem)
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
        print(new_path)
        with open(str(new_path) + "/" + file.stem + self.rewrite_template_suffixes[1] , 'w', encoding='utf-8') as new_file:
            new_file.write(content)
