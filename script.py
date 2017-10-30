from datetime import datetime
import logging
import multiprocessing
import os
from os import listdir
from os.path import isfile, join
import threading


def get_cpu_count():
    cpu_count = multiprocessing.cpu_count()
    logging.info("Number of cpu: %s" % cpu_count)
    return cpu_count


def generate_trash_dir_name():
    dir_name = "trash-%s" % datetime.today().strftime("%Y-%m-%d_%H:%M:%S.%f")
    logging.info("Trash file name: %s" % dir_name)
    return dir_name


def get_dir_structure_in_dfs(path):
    dir_list = []
    for root, directories in os.walk("."):
        for directory in directories:
            dir_list.append(root + directory)
    logging.debug("Successfully created directory structure")
    return dir_list


def create_trash_dir(path):
    os.makedirs(path)


class RemoveDuplicateFile(threading.Thread):
    def __init__(self, name, trash_path):
        threading.Thread.__init__(self)
        self.name = name
        self.trash_path = trash_path

    def extract_file_info(path):
        return [(f, os.path.getsize(join(path, f))) for f in listdir(path) if isfile(join(path, f))]

    def run(self):
        try:
            path = dir_list.pop()
        except IndexError as e:
            logging.warning("Pop from empty list")
        files = RemoveDuplicateFile.extract_file_info(path)
        for file in files:
            extract_file_info(file)


if __name__ == "__main__":
    file_info = {}
    cpu_count = get_cpu_count()
    path = input("Please enter source directory path")
    trash_dir_name = generate_trash_dir_name()
    trash_dir_path = path + trash_dir_name
    dir_list = get_dir_structure_in_dfs(path)
