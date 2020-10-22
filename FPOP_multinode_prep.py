"""
Convenience script for combinging multiple nodes of a search into a single folder for iProphet/combined FDR.
- select param files from all nodes
- finds the corresponding results folders, determines the common name, creates a common folder
- edits and copies the final shell script into the results folder so it can be started immediately
"""

import tkinter
from tkinter import filedialog
import os
import shutil
import PrepFraggerRuns

DATABASE_FILE = r"Z:\dpolasky\projects\FPOP\2020-07-03-decoys-reviewed-contam-UP000001940.fas"
# DATABASE_FILE = r"Z:\dpolasky\projects\FPOP\2020-07-03-decoys-contam-UP000001940.fas"
# DATABASE_FILE = r"Z:\dpolasky\projects\FPOP\2020-10-20-decoys-reviewed-UP000001940.fas"

RESULTS_FOLDER = '__FraggerResults'
FINAL_SHELL_PATH = r"Z:\dpolasky\projects\FPOP\copy_and_iProphet.sh"
RAWDIR = r"\\corexfs.med.umich.edu\proteomics\dpolasky\data\FPOP\Espino_PXD019290"


def edit_shell(shell_path, database_name, common_name, raw_dir):
    """
    Edit the fasta path and common folder name used by philosopher shell script
    :param shell_path: path to shell file to edit
    :type shell_path: str
    :param database_name: path to database to use - MUST BE ABS PATH for this to work (and needed to be in MSFragger search too)
    :type database_name: str
    :param common_name: common name for search used by all nodes
    :type common_name: str
    :param raw_dir: where to find raw files for linking (full path)
    :type raw_dir: str
    :return: void
    :rtype:
    """
    output = []
    with open(shell_path, 'r') as readfile:
        for line in list(readfile):
            if line.startswith('fasta'):
                line = 'fastaPath="{}"\n'.format(database_name)
            elif line.startswith('searchNameBefore'):
                line = 'searchNameBefore="{}"\n'.format(common_name)
            elif line.startswith('rawdir'):
                line = 'rawdir="{}"\n'.format(PrepFraggerRuns.update_folder_linux(raw_dir))
            output.append(line)

    # write output (linux EOL)
    with open(shell_path, 'w', newline='') as writefile:
        for line in output:
            writefile.write(line)


def create_multinode_folder(param_files):
    """
    From a set of parameter files, find the corresponding results folders, create a combined output folder, and
    copy interact pepxml into that folder
    NAMING: assumes node files are '-' delimited and 'node#' is used to distinguish between nodes, at the end of the filename
    WILL NOT WORK IF THIS NAMING CONVENTION IS NOT FOLLOWED
    :param param_files: list of param files for all nodes of 1 search
    :type param_files: list
    :return: void
    :rtype:
    """
    # create common name folder
    main_dir = os.path.dirname(param_files[0])
    splits = os.path.splitext(os.path.basename(param_files[0]))[0].split('-')
    stop_index = 0
    for index, split in enumerate(splits):
        if 'node' in split:
            # stop recording name here
            break
        # haven't reached 'node' yet, so add to name
        stop_index = index
    common_name = '-'.join(splits[:stop_index + 1])
    common_name_with_node = common_name + '-node'

    common_folder_path = os.path.join(main_dir, RESULTS_FOLDER, common_name)
    if not os.path.exists(common_folder_path):
        os.makedirs(common_folder_path)

    # copy files to common_folder_path
    # for param_file in param_files:
    #     param_name = os.path.splitext(os.path.basename(param_file))[0]
    #     results_subfolder = os.path.join(main_dir, RESULTS_FOLDER, param_name)
    #     interact_file = os.path.join(results_subfolder, 'interact.pep.xml')
    #     if not os.path.exists(interact_file):
    #         print('WARNING: PeptideProphet not yet run (output not copied) on folder {}'.format(results_subfolder))
    #         continue
    #     node_name = [x for x in param_name.split('-') if 'node' in x][0]
    #     copy_path = os.path.join(common_folder_path, '{}_interact.pep.xml'.format(node_name))
    #     print('copying file {}'.format(copy_path))
    #     shutil.copy(interact_file, copy_path)

    # copy and edit shell file for running the final analysis
    output_shell_path = os.path.join(common_folder_path, os.path.basename(FINAL_SHELL_PATH))
    shutil.copy(FINAL_SHELL_PATH, output_shell_path)
    edit_shell(output_shell_path, PrepFraggerRuns.update_folder_linux(DATABASE_FILE), common_name_with_node, RAWDIR)


if __name__ == '__main__':
    root = tkinter.Tk()
    root.withdraw()

    node_files = filedialog.askopenfilenames(filetypes=[('params', '.params')])
    create_multinode_folder(node_files)

