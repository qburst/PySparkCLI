import os


from zipfile import ZipFile
from itertools import chain


class HandleZipFiles:
    def __init__(self, filename, project):
        self.filename = filename
        self.project = project
        self.zipObj = ZipFile("{}/jobs.zip".format(project), 'w')

    def get_paths(self):
        file_paths = [files for root, directories, files in
                      os.walk("{}/src/jobs".format(self.project)) if
                      root == "{}/src/jobs".format(self.project)]
        return list(chain(*file_paths))

    def build(self):
        for path in self.get_paths():
            self.zipObj.write("{}/src/jobs/{}".format(self.project, path),
                              "jobs/{}".format(path))
        self.zipObj.close()
