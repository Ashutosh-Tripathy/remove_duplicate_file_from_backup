from datetime import datetime
import logging
import multiprocessing
import os
from os import listdir
from os.path import isfile, join
import threading

logging.basicConfig(
    filename="output.log",
    level=logging.DEBUG,
    format='[%(levelname)s] (%(threadName)-10s) %(message)s',
    filemode='w'
)


def get_cpu_count():
    cpu_count = multiprocessing.cpu_count()
    logging.info("Number of cpu: %s" % cpu_count)
    return cpu_count


def trash_directory_already_present(path):
    directories = [d for d in os.listdir(
        path) if os.path.isdir(os.path.join(path, d))]
    for directory in directories:
        if directory.startswith('trash-'):
            logging.error(
                "%s: already have a folder starting with 'trash-'. Remove this folder before running again." % path)
            return True
    return False


def generate_trash_dir_name():
    dir_name = "trash-%s" % datetime.today().strftime("%Y-%m-%d-%H-%M-%S-%f")
    logging.info("Trash file name: %s" % dir_name)
    return dir_name


def get_dir_structure_in_dfs(path):
    dir_list = []
    for root, directories, files in os.walk(path):
        for directory in directories:
            dir_list.append(root + directory)
    dir_list.insert(0, path)
    logging.debug("Successfully created directory structure")
    return dir_list


def create_trash_dir(path):
    os.makedirs(path)


class RemoveDuplicateFile(threading.Thread):
    def __init__(self, name, trash_path):
        threading.Thread.__init__(self)
        self.name = name
        self.trash_path = trash_path

    def extract_file_info(self, path):
        return {f: (os.path.getsize(join(path, f)), join(path, f)) for f in listdir(path)
                if isfile(join(path, f))}

    def move_file_to_trash(self, old_path, file):
        os.rename(old_path, join(self.trash_path, file))

    def run(self):
        while len(global_dir_list) > 0:
            try:
                dir_path = global_dir_list.pop(0)
            except IndexError as e:
                logging.warning("%s-Pop from empty list" % (self.name))
            file_info_dict = self.extract_file_info(dir_path)
            logging.debug("file_info_dict")
            logging.debug(file_info_dict)
            logging.debug(dir_path)
            for file in file_info_dict:
                detail = file_info_dict[file]
                if file in global_file_detail and detail[0] == global_file_detail[file][0]:
                    logging.info("%s-Found duplicate file: %s | origianl: %s | duplicate: %s" % (
                        self.name, file, global_file_detail[file][1], detail[1]))
                    self.move_file_to_trash(detail[1], file)
                else:
                    global_file_detail[file] = file_info_dict[file]


if __name__ == "__main__":
    global_file_detail = {}
    cpu_count = get_cpu_count()
    # path = input("Please enter source directory path: ")
    path = 'd:/pers/test/'
    trash_dir_name = generate_trash_dir_name()
    trash_dir_path = path + trash_dir_name
    global_dir_list = get_dir_structure_in_dfs(path)
    if not trash_directory_already_present(path):
        create_trash_dir(trash_dir_path)
        threads = []
        for i in range(1, cpu_count + 1):
            thread = RemoveDuplicateFile("Thread-%d" % (i), trash_dir_path)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
        logging.debug("global_file_detail")
        logging.debug(global_file_detail)
    logging.debug("Completed!!")
