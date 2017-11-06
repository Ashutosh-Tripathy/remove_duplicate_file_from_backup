from datetime import datetime
import logging
import multiprocessing
import os
from os import listdir
from os.path import isfile, join
import threading
import uuid


lock = threading.Lock()


class DuplicateKeyError(KeyError):
    pass


class UniqueKeyDict(dict):
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)

    def __setitem__(self, key, value):
        with lock:
            if key in self:
                raise DuplicateKeyError("%s is already in dict" % key)
            super(UniqueKeyDict, self).__setitem__(key, value)


def initialize_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to info
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '[%(levelname)s] (%(threadName)-10s) %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # create debug file handler and set level to debug
    handler = logging.FileHandler("output.log", "w")
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '[%(levelname)s] (%(threadName)-10s) %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


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
                "%s already has a folder starting with 'trash-'. Remove this folder before running again." % path)
            return True
    return False


def generate_trash_dir_name():
    dir_name = "trash-%s" % datetime.today().strftime("%Y-%m-%d-%H-%M-%S")
    logging.info("Trash file name: %s" % dir_name)
    return dir_name


def get_dir_structure_in_dfs(path):
    dir_list = []
    for root, directories, files in os.walk(path):
        for directory in directories:
            dir_list.append(join(root, directory))
    dir_list.insert(0, path)
    logging.debug("Successfully scanned directory structure")
    return dir_list


def create_trash_dir(path):
    os.makedirs(path)


class RemoveDuplicateFile(threading.Thread):
    def __init__(self, name, trash_path):
        threading.Thread.__init__(self)
        self.name = name
        self.trash_path = trash_path

    def extract_file_info(self, path):
        # {"filename":("size", "path")}
        return {f: (os.path.getsize(join(path, f)), join(path, f)) for f in listdir(path)
                if isfile(join(path, f))}

    def move_file_to_trash(self, source, file):
        target = join(self.trash_path, file)
        try:
            os.rename(source, target)
        except Exception:
            logging.warn(
                "File: %s already present in diectory: %s" % (file, target))
            filename, fileext = os.path.splitext(file)
            target = join(self.trash_path, filename +
                          "-" + str(uuid.uuid4()) + fileext)
            os.rename(source, target)
        logging.info("Moving file: %s | source : %s | target : %s" %
                     (file, source, target))

    def run(self):
        while len(global_dir_list) > 0:
            try:
                dir_path = global_dir_list.pop(0)
            except IndexError as e:
                logging.warning("Pop from empty list")
            logging.debug("scan files in directory: %s" % dir_path)
            file_info_dict = self.extract_file_info(dir_path)
            for file in file_info_dict:
                detail = file_info_dict[file]
                try:
                    global_file_detail[file] = file_info_dict[file]
                except DuplicateKeyError:
                    if detail[0] == global_file_detail[file][0]:
                        logging.info("Found duplicate file: %s | origianl: %s | duplicate: %s" % (
                            file, global_file_detail[file][1], detail[1]))
                        self.move_file_to_trash(detail[1], file)


if __name__ == "__main__":
    initialize_logger()
    path = input("Please enter source directory path: ")
    path = path if path[-1] == '/' else path + "/"
    if not os.path.exists(path):
        msg = "Invalid path"
        logging.error(msg)
        raise ValueError(msg)
    global_file_detail = UniqueKeyDict()
    cpu_count = get_cpu_count()
    # path = 'd:/pers/test/'
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
    logging.debug("Completed!!")
