import os


from zipfile import ZipFile
from itertools import chain


class HandleZipFiles:
    def __init__(self, filenames, project):
        self.filenames = filenames
        self.project = project

    def get_paths(self, name):
        path = "{}/src/{}".format(self.project, name)
        file_paths = [files for root, directories, files in os.walk(path)
                      if root == path]
        return list(chain(*file_paths))

    def build(self):
        for name in self.filenames.split(","):
            self.zipObj = ZipFile(name, 'w')
            zip_name = name.split(".")[0].split("/")[-1]
            for path in self.get_paths(zip_name):
                self.zipObj.write("{}/src/{}/{}".format(self.project, zip_name, path),
                                  "{}/{}".format(zip_name, path))
            self.zipObj.close()
