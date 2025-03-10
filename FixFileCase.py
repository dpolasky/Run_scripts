"""
Quick script to change all the file names in my Fragger (or etc) source to quickly reset capitalization errors
in the IDE.
"""

import os
import tkinter
from tkinter import messagebox

EDIT_DIR = r"C:\Users\dpolasky\Repositories\msfragger\src\edu\umich\andykong\msfragger"
# EDIT_DIR = r"C:\Users\dpolasky\IdeaProjects\msfragger\src\edu\umich\andykong\msfragger"
# EDIT_DIR = r"C:\Users\dpolasky\GitRepositories\FragPipe\FragPipe\FragPipe-GUI"


def single_folder(edit_dir):
    """
    MSFragger original method
    :param edit_dir:
    :type edit_dir:
    :return:
    :rtype:
    """
    # rename files (add 'a' to the start)
    files = [os.path.join(edit_dir, x) for x in os.listdir(edit_dir) if x.endswith('.java')]
    for file in files:
        new_filename = 'a' + os.path.basename(file)
        new_filepath = os.path.join(edit_dir, new_filename)
        os.rename(file, new_filepath)

    # prompt user to click stuff in the IDE
    messagebox.showinfo('Click the IDE!', 'Click on the files in the IDE, then close this prompt (files will be renamed back to originals')

    # fix the filenames (remove 'a' form the start)
    edited_files = [os.path.join(edit_dir, x) for x in os.listdir(edit_dir) if x.endswith('.java')]
    for file in edited_files:
        new_filename = os.path.basename(file)[1:]
        new_filepath = os.path.join(edit_dir, new_filename)
        os.rename(file, new_filepath)


def include_subdirs(edit_dir):
    """
    multi-depth version for Fragpipe. NOTE: have not ever had to actually use this (cause was build.gradle file not being updated...)
    :param edit_dir:
    :type edit_dir:
    :return:
    :rtype:
    """
    # rename files (add 'a' to the start)
    # files = [os.path.join(edit_dir, x) for x in os.listdir(edit_dir) if x.endswith('.java')]
    for dirpath, dirnames, filenames in os.walk(edit_dir):
        for filename in filenames:
            if not dirpath.startswith('.idea') or dirpath.startswith('.gradle'):
                if not filename.endswith('.xml'):
                    # if filename.endswith('.java') or filename.endswith('.jar'):
                    new_filename = '$' + os.path.basename(filename)
                    new_filepath = os.path.join(dirpath, new_filename)
                    os.rename(os.path.join(dirpath, filename), new_filepath)
                    print('renaming: {}'.format(filename))

    # prompt user to click stuff in the IDE
    messagebox.showinfo('Click the IDE!',
                        'Click on the files in the IDE, then close this prompt (files will be renamed back to originals')

    # fix the filenames (remove 'a' form the start)
    # edited_files = [os.path.join(edit_dir, x) for x in os.listdir(edit_dir) if x.endswith('.java')]
    # for file in edited_files:
    #     new_filename = os.path.basename(file)[1:]
    #     new_filepath = os.path.join(edit_dir, new_filename)
    #     os.rename(file, new_filepath)
    for dirpath, dirnames, filenames in os.walk(edit_dir):
        for filename in filenames:
            if not dirpath.startswith('.idea') or dirpath.startswith('.gradle'):
                if not filename.endswith('.xml'):
                    if filename.startswith('$'):
                        new_filename = os.path.basename(filename)[1:]
                    else:
                        new_filename = filename
                    new_filepath = os.path.join(dirpath, new_filename)
                    try:
                        os.rename(os.path.join(dirpath, filename), new_filepath)
                    except FileExistsError:
                        continue
                    print('renaming back: {}'.format(filename))


if __name__ == '__main__':
    root = tkinter.Tk()
    root.withdraw()

    single_folder(EDIT_DIR)
