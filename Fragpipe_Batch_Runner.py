"""
Utility for running FragPipe batches from headless mode using a template file (csv).
Uses windows filechooser to select template file, but could generalize instead
"""

import tkinter
from tkinter import filedialog
import os
import shutil
from enum import Enum
import datetime


FRAGPIPE_PATH = r"\\corexfs.med.umich.edu\proteomics\dpolasky\tools\_FragPipes\a_current\bin\fragpipe"
# FRAGPIPE_PATH = r"C:\Users\dpolasky\GitRepositories\FragPipe\FragPipe\MSFragger-GUI\build\install\fragpipe\bin\fragpipe.exe"
USE_LINUX = True
# DISABLE_TOOLS = True
DISABLE_TOOLS = False
BATCH_INCREMENT = ''    # set to '2' (or higher) for multiple batches in same folder
OUTPUT_FOLDER_APPEND = '__FraggerResults'
FILETYPES_FOR_COPY = ['pepXML']
# FILETYPES_FOR_COPY = ['pepXML', 'pin']


class DisableTools(Enum):
    """
    tool names that match the run-[TOOL] format of the workflow parameters
    """
    MSFRAGGER = 'msfragger'
    PROTEINPROPHET = 'protein-prophet'
    PEPTIDEPROPHET = 'peptide-prophet'
    PTMPROPHET = 'ptmprophet'
    PTMSHEPHERD = 'shepherd'
    VALIDATION = 'psm-validation'       # note: need to use this also if turning off PP/Perc
    TMTINTEGRATOR = 'tmtintegrator'
    PERCOLATOR = 'percolator'
    FILTERandREPORT = 'report'


# TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER]
# TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER, DisableTools.PEPTIDEPROPHET, DisableTools.PERCOLATOR, DisableTools.PROTEINPROPHET, DisableTools.VALIDATION]     # filter/report and PTM-S or quant only
# TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER, DisableTools.PEPTIDEPROPHET, DisableTools.PERCOLATOR, DisableTools.PROTEINPROPHET, DisableTools.VALIDATION, DisableTools.FILTERandREPORT]     # PTM-S or quant only
# TOOLS_TO_DISABLE = [DisableTools.PTMPROPHET]
# TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER, DisableTools.PEPTIDEPROPHET, DisableTools.VALIDATION, DisableTools.PTMPROPHET]
TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER, DisableTools.PTMPROPHET]

if not DISABLE_TOOLS:
    TOOLS_TO_DISABLE = None


class FragpipeRun(object):
    """
    container for run info
    """
    workflow_path: str
    manifest_path: str
    output_path: str
    ram: str
    threads: str
    msfragger_path: str
    philosopher_path: str
    python_path: str
    skip_msfragger_path: str

    def __init__(self, workflow, manifest, output, ram, threads, msfragger, philosopher, python=None, skip_MSFragger=None, disable_list=None):
        if output == '':
            # use base workflow name automatically if no specific output name specified
            output_name = os.path.join(OUTPUT_FOLDER_APPEND, os.path.basename(os.path.splitext(workflow)[0]))
        else:
            output_name = output

        # update output_dir to full path (template has only the unique name, not the full path), and make dir if it doesn't exist
        self.output_path = os.path.join(os.path.dirname(workflow), output_name)
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)

        # copy workflow file to output dir (for later reference)
        self.workflow_path = os.path.join(self.output_path, os.path.basename(workflow))
        shutil.copy(workflow, self.workflow_path)

        self.manifest_path = manifest
        self.ram = ram
        self.threads = threads
        self.msfragger_path = msfragger
        self.philosopher_path = philosopher
        if python is not None:
            self.python_path = python
        else:
            self.python_path = None

        if skip_MSFragger is not None:
            # disable the MSFragger run in this workflow and note the path to copy from for adding to the shell script
            edit_workflow_disable_tools(self.workflow_path, [DisableTools.MSFRAGGER])
            if output == '':
                # add fragger results folder append
                if skip_MSFragger.endswith('.workflow') or skip_MSFragger.endswith('.workflow\n'):
                    final_folder = os.path.basename(os.path.splitext(skip_MSFragger)[0])
                else:
                    final_folder = os.path.basename(skip_MSFragger)
                self.skip_msfragger_path = os.path.join(os.path.dirname(skip_MSFragger), OUTPUT_FOLDER_APPEND, final_folder)
            else:
                if skip_MSFragger.endswith('.workflow') or skip_MSFragger.endswith('.workflow\n'):
                    skip_MSFragger = os.path.splitext(skip_MSFragger)[0]
                self.skip_msfragger_path = skip_MSFragger
        else:
            self.skip_msfragger_path = None

        if disable_list is not None:
            edit_workflow_disable_tools(self.workflow_path, disable_list)

    def update_linux(self):
        """
        update all paths to be linux-ized, AND update manifest file paths
        :return: void
        :rtype:
        """
        # update paths in specific files
        self.manifest_path = update_manifest_linux(self.manifest_path)
        update_workflow_linux(self.workflow_path)

        # update the paths themselves
        self.workflow_path = update_folder_linux(self.workflow_path)
        self.manifest_path = update_folder_linux(self.manifest_path)
        self.output_path = update_folder_linux(self.output_path)
        self.msfragger_path = update_folder_linux(self.msfragger_path)
        self.philosopher_path = update_folder_linux(self.philosopher_path)
        if self.python_path is not None:
            self.python_path = update_folder_linux(self.python_path)


def update_manifest_linux(manifest_path):
    """
    update the manifest file to linux paths and save a copy, return the updated path to use as new manifest path
    :param manifest_path: full path to manifest file
    :type manifest_path: str
    :return: updated path
    :rtype: str
    """
    output = []
    with open(manifest_path, 'r') as readfile:
        for line in list(readfile):
            newline = update_folder_linux(line)
            output.append(newline)
    pathsplits = os.path.splitext(manifest_path)
    newpath = pathsplits[0] + '_linux' + pathsplits[1]
    with open(newpath, 'w', newline='') as outfile:
        for line in output:
            outfile.write(line)
    return newpath


def edit_workflow_disable_tools(workflow_path, disable_list):
    """
    Edit the workflow file to change the specified tools to not be run
    :param workflow_path: full path to workflow file
    :type workflow_path: str
    :param disable_list: list of tool names to disable (specified in Enum)
    :type disable_list: list
    :return: void
    :rtype:
    """
    output = []
    with open(workflow_path, 'r') as readfile:
        for line in list(readfile):
            newline = line
            for tool_name in disable_list:
                if 'run-{}'.format(tool_name.value) in line:
                    # change to false
                    splits = line.split('=')
                    newline = splits[0] + '=false\n'
            output.append(newline)
    with open(workflow_path, 'w') as outfile:
        for line in output:
            outfile.write(line)


def update_workflow_linux(workflow_path):
    """
    update the path to the database file to linux path
    :param workflow_path: path to workflow file
    :type workflow_path: str
    :return: void
    :rtype:
    """
    output = []
    # read to edit database path
    with open(workflow_path, 'r') as readfile:
        for line in list(readfile):
            if line.startswith('database.db-path'):
                newline = update_folder_linux(line)
            elif line.startswith('ptmshepherd.glycodatabase'):
                newline = update_folder_linux(line)
            else:
                newline = line
            output.append(newline)

    # save output back to same path
    with open(workflow_path, 'w') as outfile:
        for line in output:
            outfile.write(line)


def parse_template(template_file, disable_list):
    """
    read the template into a list of FragpipeRun containers
    :param template_file: full path to template file to read
    :type template_file: str
    :param disable_list: list of tool names to disable (specified in Enum)
    :type disable_list: list
    :return: list of FragpipeRun containers
    :rtype: list[FragpipeRun]
    """
    runs = []
    with open(template_file, 'r') as readfile:
        for line in list(readfile):
            if line.startswith('#'):
                continue
            splits = [x for x in line.split(',') if x is not '\n']
            this_run = FragpipeRun(*splits, disable_list=disable_list)
            runs.append(this_run)
    return runs


def make_commands_linux(run_list, fragpipe_path, output_path):
    """
    Format commands and write to linux shell script from the provided run list
    :param run_list: list of runs
    :type run_list: list[FragpipeRun]
    :param fragpipe_path: full path to fragpipe executable
    :type fragpipe_path: str
    :param output_path: full path to save output file
    :type output_path: str
    :return: void
    :rtype:
    """
    batch_path = os.path.join(output_path, 'fragpipe_batch{}.sh'.format(BATCH_INCREMENT))
    linux_fragpipe = update_folder_linux(fragpipe_path)
    with open(batch_path, 'w', newline='') as outfile:
        outfile.write('#!/bin/bash\nset -xe\n\n')   # header
        for fragpipe_run in run_list:
            fragpipe_run.update_linux()
            current_time = datetime.datetime.now()
            log_path = '{}/log-fragpipe_{}.txt'.format(fragpipe_run.output_path, current_time.strftime("%Y-%m-%d_%H-%M-%S"))
            arg_list = [linux_fragpipe,
                        fragpipe_run.workflow_path,
                        fragpipe_run.manifest_path,
                        fragpipe_run.output_path,
                        fragpipe_run.ram,
                        fragpipe_run.threads,
                        fragpipe_run.msfragger_path,
                        fragpipe_run.philosopher_path,
                        log_path
                        ]
            if fragpipe_run.skip_msfragger_path is not None:
                for filetype_str in FILETYPES_FOR_COPY:
                    outfile.write('cp {}/*.{} {}\n'.format(update_folder_linux(fragpipe_run.skip_msfragger_path), filetype_str, update_folder_linux(fragpipe_run.output_path)))
                # outfile.write('cp {}/*.pin {}\n'.format(update_folder_linux(fragpipe_run.skip_msfragger_path), update_folder_linux(fragpipe_run.output_path)))
            outfile.write('{} --headless --workflow {} --manifest {} --workdir {} --ram {} --threads {} --config-msfragger {} --config-philosopher {} |& tee {}\n'.format(*arg_list))


def make_commands_windows(run_list, fragpipe_path, output_path):
    """
    Format commands and write to windows bat file from the provided run list
    :param run_list: list of runs
    :type run_list: list[FragpipeRun]
    :param fragpipe_path: full path to fragpipe executable
    :type fragpipe_path: str
    :param output_path: full path to save output file
    :type output_path: str
    :return: void
    :rtype:
    """
    batch_path = os.path.join(output_path, 'fragpipe_batch.bat')
    with open(batch_path, 'w') as outfile:
        for fragpipe_run in run_list:
            arg_list = [fragpipe_path,
                        fragpipe_run.workflow_path,
                        fragpipe_run.manifest_path,
                        fragpipe_run.output_path,
                        fragpipe_run.ram,
                        fragpipe_run.threads,
                        fragpipe_run.msfragger_path,
                        fragpipe_run.philosopher_path
                        ]
            outfile.write('{} --headless --workflow {} --manifest {} --workdir {} --ram {} --threads {} --config-msfragger {} --config-philosopher {}\n'.format(*arg_list))


def main(template_file, fragpipe_path, write_to_linux, disable_list):
    """
    Run a FragPipe batch from the template file. Template format: workflow, manifest, output \n, one analysis per line.
    Writes Windows batch file or linux shell script to run in the same path as the template file
    :param template_file: full path to template file to read
    :type template_file: str
    :param fragpipe_path: full path to fragpipe executable
    :type fragpipe_path: str
    :param disable_list: list of tool names to disable (specified in Enum)
    :type disable_list: list
    :param write_to_linux: if true, update paths to be linux output rather than windows
    :type write_to_linux: bool
    :return: void
    :rtype:
    """
    run_list = parse_template(template_file, disable_list)
    output_dir = os.path.dirname(template_file)
    if write_to_linux:
        make_commands_linux(run_list, fragpipe_path, output_dir)
    else:
        make_commands_windows(run_list, fragpipe_path, output_dir)


def update_folder_linux(folder_name):
    """
    helper to auto update windows directories to linux
    :param folder_name: path to update
    :return: updated path
    """
    if folder_name.startswith('//corexfs'):
        linux_name = folder_name.replace('//corexfs.med.umich.edu/proteomics', '/storage')
        linux_name = linux_name.replace('\\', '/')
    elif folder_name.startswith('\\\\corexfs'):
        folder_name = folder_name.replace('\\', '/')
        linux_name = folder_name.replace('//corexfs.med.umich.edu/proteomics', '/storage')
    elif folder_name.startswith('Z:'):
        linux_name = folder_name.replace('Z:', '/storage')
        linux_name = linux_name.replace('\\', '/')
    elif 'Z\\:' in folder_name:
        linux_name = folder_name.replace('Z\\:', '/storage')
        linux_name = linux_name.replace('\\', '/')
        linux_name = linux_name.replace('//', '/')
    elif 'C\\:' in folder_name:
        linux_name = folder_name.replace('C\\:', '')
        linux_name = linux_name.replace('\\', '/')
        linux_name = linux_name.replace('//', '/')
    else:
        linux_name = folder_name
    return linux_name


if __name__ == '__main__':
    root = tkinter.Tk()
    root.withdraw()

    template = filedialog.askopenfilename(filetypes=[('FP Template', '.csv')])
    main(template, FRAGPIPE_PATH, USE_LINUX, TOOLS_TO_DISABLE)
    print('Done!')
