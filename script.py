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
        return {f: (os.path.getsize(join(path, f)), join(path, f)) for f in listdir(path)
                if isfile(join(path, f))}

    def move_file_to_trash(self, old_path):
        os.rename(old_path, self.trash_path)

    def run(self):
        try:
            path = dir_list.pop()
        except IndexError as e:
            logging.warning("Pop from empty list")
        file_info_dict = RemoveDuplicateFile.extract_file_info(path)
        for file, detail in file_info_dict:
            if file in global_file_detail:
                if detail[0] == global_file_detail[file][0]:
                    logging.info("Found duplicate file: %s origianl: %s duplicate: %s" % (
                        file, global_file_detail[file][1], detail[1]))
                    self.move_file_to_trash(detail[1])
            else:
                global_file_detail[file] = file_info_dict[file]



if __name__ == "__main__":
    global_file_detail = {}
    cpu_count = get_cpu_count()
    path = input("Please enter source directory path")
    trash_dir_name = generate_trash_dir_name()
    trash_dir_path = path + trash_dir_name
    dir_list = get_dir_structure_in_dfs(path)
