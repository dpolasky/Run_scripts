"""
Utility for running FragPipe batches from headless mode using a template file (csv).
Uses windows filechooser to select template file, but could generalize instead
"""
import pathlib
import re
import tkinter
from tkinter import filedialog
import os
import shutil
from enum import Enum
import datetime


FRAGPIPE_PATH = r"\\corexfs.med.umich.edu\proteomics\dpolasky\tools\_FragPipes\a_current\bin\fragpipe"
# FRAGPIPE_PATH = r"\\corexfs.med.umich.edu\proteomics\dpolasky\tools\_FragPipes\current2\bin\fragpipe"
# FRAGPIPE_PATH = r"\\corexfs.med.umich.edu\proteomics\dpolasky\tools\_FragPipes\21.1\fragpipe\bin\fragpipe"
# FRAGPIPE_PATH = r"Z:\dpolasky\tools\_FragPipes\19.0-patch-version-comp\bin\fragpipe"
# FRAGPIPE_PATH = r"Z:\dpolasky\tools\_FragPipes\UCLA-tags\bin\fragpipe"
# FRAGPIPE_PATH = r"C:\Users\dpolasky\GitRepositories\FragPipe\FragPipe\MSFragger-GUI\build\install\fragpipe\bin\fragpipe.exe"

NEW_FRAGPIPE = True     # if using FragPipe newer than 21.2-build41, passing tools folder instead of individual tool paths

USE_LINUX = True
# DISABLE_TOOLS = True
DISABLE_TOOLS = False
BATCH_INCREMENT = ''    # set to '2' (or higher) for multiple batches in same folder
OUTPUT_FOLDER_APPEND = '__FraggerResults'

DEFAULT_TOOLS_PATH = r"Z:\dpolasky\tools"
TEMP_TOOLS_NAME = "temp_tools"
TEMP_TOOLS_FOLDERS = []
DIA_TRACER_PATH = r"Z:\dpolasky\tools\diaTracer-1.1.3.jar"      # not implemented to change versions of this, just needed for temp tools copying

# NOTE: some tools are always disabled (see below) if copying - check there if you need PeptideProphet/etc after copying
# FILETYPES_FOR_COPY = ['pepXML']
# FILETYPES_FOR_COPY = ['pepXML', 'pin']
# FILETYPES_FOR_COPY = ['.pep.xml', '.prot.xml', '_opair.txt']
FILETYPES_FOR_COPY = ['.pep.xml', '.prot.xml']


class DisableTools(Enum):
    """
    tool names that match the run-[TOOL] format of the workflow parameters
    """
    MSFRAGGER = 'msfragger'
    PROTEINPROPHET = 'protein-prophet'
    PEPTIDEPROPHET = 'peptide-prophet'
    PTMPROPHET = 'ptmprophet'
    PTMSHEPHERD = 'shepherd'
    PSMVALIDATION = 'psm-validation'       # note: need to use this also if turning off PP/Perc
    TMTINTEGRATOR = 'tmtintegrator'
    PERCOLATOR = 'percolator'
    FILTERandREPORT = 'report'
    FREEQUANT = 'freequant'
    LFQ = 'label-free-quant'        # note: must also be used with FreeQuant/IonQuant to disable
    DIANN = 'dia-nn'
    SPECLIB = 'speclibgen'
    OPAIR = 'opair'


# TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER]
# TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER, DisableTools.PEPTIDEPROPHET, DisableTools.PERCOLATOR, DisableTools.PSMVALIDATION]
# filter/report onwards (PTM-S, OPair, quant)
# TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER, DisableTools.PEPTIDEPROPHET, DisableTools.PERCOLATOR, DisableTools.PROTEINPROPHET, DisableTools.PSMVALIDATION]
TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER, DisableTools.PEPTIDEPROPHET, DisableTools.PERCOLATOR, DisableTools.PROTEINPROPHET, DisableTools.PSMVALIDATION, DisableTools.FILTERandREPORT]     # PTM-S or quant only
# TOOLS_TO_DISABLE = [DisableTools.PTMPROPHET]
# TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER, DisableTools.PEPTIDEPROPHET, DisableTools.PERCOLATOR, DisableTools.PSMVALIDATION, DisableTools.PTMPROPHET]
# TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER, DisableTools.PTMPROPHET]
# TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER, DisableTools.PEPTIDEPROPHET, DisableTools.PSMVALIDATION, DisableTools.PERCOLATOR]
# TOOLS_TO_DISABLE = [DisableTools.FREEQUANT, DisableTools.LFQ]
# OPair, quant only
# TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER, DisableTools.PEPTIDEPROPHET, DisableTools.PERCOLATOR, DisableTools.PROTEINPROPHET, DisableTools.PSMVALIDATION, DisableTools.FILTERandREPORT, DisableTools.PTMSHEPHERD]
# speclib only
# TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER, DisableTools.PEPTIDEPROPHET, DisableTools.PERCOLATOR, DisableTools.PROTEINPROPHET, DisableTools.PSMVALIDATION, DisableTools.FILTERandREPORT, DisableTools.PTMSHEPHERD, DisableTools.OPAIR]
# TOOLS_TO_DISABLE = [DisableTools.SPECLIB, DisableTools.DIANN]

if not DISABLE_TOOLS:
    TOOLS_TO_DISABLE = None

# always disable if copying results files over (default: start at Filter)
DISABLE_IF_COPY = [DisableTools.MSFRAGGER, DisableTools.PEPTIDEPROPHET, DisableTools.PERCOLATOR, DisableTools.PROTEINPROPHET, DisableTools.PSMVALIDATION, DisableTools.PTMPROPHET]
# DISABLE_IF_COPY = [DisableTools.MSFRAGGER]

TEMPLATE_COLS = {
    'fragpipe_path': 0,
    'workflow_path': 2,
    'manifest_path': 2,
    'output_path': 3,
    'ram': 4,
    'threads': 5,
    'msfragger_path': 6,
    'philosopher_path': 7,
    'ionquant_path': 8,
    'python_path': 9,
    'skip_msfragger_path': 10,
    'database_path': 11
}


class FragpipeRun(object):
    """
    container for run info
    """
    fragpipe_path: str
    workflow_path: str
    manifest_path: str
    output_path: str
    original_output_path: str
    ram: str
    threads: str
    msfragger_path: str
    philosopher_path: str
    ionquant_path: str
    python_path: str
    skip_msfragger_path: str
    database_path: str

    def __init__(self, fragpipe, workflow, manifest, output, ram, threads, msfragger, philosopher, ionquant, python=None, skip_MSFragger=None, database_path=None, disable_list=None):
        if output == '':
            # use base workflow name automatically if no specific output name specified
            output_name = os.path.join(OUTPUT_FOLDER_APPEND, os.path.basename(os.path.splitext(workflow)[0]))
        else:
            output_name = output

        # update output_dir to full path (template has only the unique name, not the full path), and make dir if it doesn't exist
        self.output_path = os.path.join(os.path.dirname(workflow), output_name)
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)
        self.original_output_path = self.output_path

        # copy workflow file to output dir (for later reference)
        self.workflow_path = os.path.join(self.output_path, os.path.basename(workflow))
        shutil.copy(workflow, self.workflow_path)

        self.fragpipe_path = fragpipe
        self.manifest_path = manifest
        self.ram = ram
        self.threads = threads
        self.msfragger_path = msfragger
        self.philosopher_path = philosopher
        self.ionquant_path = ionquant
        if python is not None:
            self.python_path = python
        else:
            self.python_path = None

        if skip_MSFragger is not None and skip_MSFragger != '':
            # disable the MSFragger run in this workflow and note the path to copy from for adding to the shell script
            edit_workflow_disable_tools(self.workflow_path, DISABLE_IF_COPY)
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
        if database_path is not None:
            self.database_path = database_path
            if USE_LINUX:
                self.database_path = update_folder_linux(self.database_path)
            self.edit_database()

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
        self.fragpipe_path = update_folder_linux(self.fragpipe_path)
        self.workflow_path = update_folder_linux(self.workflow_path)
        self.manifest_path = update_folder_linux(self.manifest_path)
        self.output_path = update_folder_linux(self.output_path)
        self.msfragger_path = update_folder_linux(self.msfragger_path)
        self.philosopher_path = update_folder_linux(self.philosopher_path)
        self.ionquant_path = update_folder_linux(self.ionquant_path)
        if self.python_path is not None:
            self.python_path = update_folder_linux(self.python_path)

    def edit_database(self):
        """
        Add (or edit) the database with provided override path from the template
        :return: void
        :rtype:
        """
        output = []
        found_db = False
        index = 0
        with open(self.workflow_path, 'r') as readfile:
            for line in list(readfile):
                if line.startswith('database.db-path'):
                    newline = 'database.db-path={}\n'.format(self.database_path)
                    found_db = True
                elif line.startswith('crystalc') and not found_db:     # db-path is not in the file, would have been found already
                    output.append('database.db-path={}\n'.format(self.database_path))
                    newline = line
                    found_db = True
                else:
                    newline = line
                output.append(newline)
                index += 1
        with open(self.workflow_path, 'w') as outfile:
            for line in output:
                outfile.write(line)

    def has_no_tool_paths(self):
        """
        check if all tool paths are specified
        :return:
        """
        return self.msfragger_path == '' and self.ionquant_path == '' and self.philosopher_path == ''


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


def update_manifest_windows(manifest_path):
    """
    update the manifest file to Windows paths and save in place
    :param manifest_path: full path to manifest file
    :type manifest_path: str
    :return:
    """
    output = []
    with open(manifest_path, 'r') as readfile:
        for line in list(readfile):
            newline = update_folder_windows(line)
            output.append(newline)
    with open(manifest_path, 'w') as outfile:
        for line in output:
            outfile.write(line)


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
            elif line.startswith('ptmshepherd.opair.glyco_db') or line.startswith('opair.glyco_db'):
                newline = update_folder_linux(line)
            elif line.startswith('opair.oxonium_filtering_file'):
                newline = update_folder_linux(line)
            elif line.startswith('msfragger.mass_offset_file'):
                newline = update_folder_linux(line)
            elif line.startswith('mbg.glycan_db'):
                newline = update_folder_linux(line)
            else:
                newline = line
            output.append(newline)

    # save output back to same path
    with open(workflow_path, 'w') as outfile:
        for line in output:
            outfile.write(line)


def parse_template(template_file, disable_list, fragpipe_path):
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
            splits = [x for x in line.split(',') if x != '\n']
            splits[-1] = splits[-1].rstrip('\n')
            splits.insert(0, fragpipe_path)
            this_run = FragpipeRun(*splits, disable_list=disable_list)
            runs.append(this_run)
    return runs


def make_commands_linux(run_list, output_path, fragpipe_uses_tools_folder, write_output=True, is_first_run=True):
    """
    Format commands and write to linux shell script from the provided run list
    :param run_list: list of runs
    :type run_list: list[FragpipeRun]
    :param output_path: full path to save output file
    :type output_path: str
    :return: void
    :rtype:
    """
    batch_path = os.path.join(output_path, 'fragpipe_batch{}.sh'.format(BATCH_INCREMENT))
    output = []
    if is_first_run:
        output.append('#!/bin/bash\nset -xe\n\n')   # bash header
        if fragpipe_uses_tools_folder:
            delete_old_temp_tools()

    for fragpipe_run in run_list:
        # copy original format manifest file to output dir before updating paths [disabled after 18.1 update fixes manifest copying]
        # shutil.copy(fragpipe_run.manifest_path, os.path.join(fragpipe_run.output_path, os.path.basename(fragpipe_run.manifest_path)))

        current_time = datetime.datetime.now()
        log_path = '{}/log-fragpipe_{}.txt'.format(update_folder_linux(fragpipe_run.output_path), current_time.strftime("%Y-%m-%d_%H-%M-%S"))
        if fragpipe_run.skip_msfragger_path is not None:
            for filetype_str in FILETYPES_FOR_COPY:
                # link necessary file types from the copied analysis. Check if the needed files exist (from a previous attempt at the run) and write commands to link if not
                if not any(file.endswith(filetype_str) for file in os.listdir(fragpipe_run.original_output_path)):
                    output.append('ln -s {}/*{} {}\n'.format(update_folder_linux(fragpipe_run.skip_msfragger_path), filetype_str, update_folder_linux(fragpipe_run.output_path)))

        python_arg = ''
        if len(fragpipe_run.python_path) > 0:
            python_arg = ' --config-python {}'.format(update_folder_linux(fragpipe_run.python_path))
        if fragpipe_uses_tools_folder:
            arg_list = []
            if fragpipe_run.has_no_tool_paths():
                # no special versions desired - use the default tools folder for pathing
                tools_path = DEFAULT_TOOLS_PATH
            else:
                # special versions requested. Copy them to a new subfolder and pass that
                temp_tools_name = "{}_{}".format(TEMP_TOOLS_NAME, len(TEMP_TOOLS_FOLDERS) + 1)
                temp_tools_path = pathlib.Path(DEFAULT_TOOLS_PATH) / temp_tools_name
                os.makedirs(temp_tools_path)
                TEMP_TOOLS_FOLDERS.append(temp_tools_path)
                tools_path = str(temp_tools_path)
                shutil.copy(fragpipe_run.msfragger_path, temp_tools_path / os.path.basename(fragpipe_run.msfragger_path))
                shutil.copy(fragpipe_run.ionquant_path, temp_tools_path / os.path.basename(fragpipe_run.ionquant_path))
                shutil.copy(DIA_TRACER_PATH, temp_tools_path / os.path.basename(DIA_TRACER_PATH))

            fragpipe_run.update_linux()
            arg_list = [fragpipe_run.fragpipe_path,
                        fragpipe_run.workflow_path,
                        fragpipe_run.manifest_path,
                        fragpipe_run.output_path,
                        fragpipe_run.ram,
                        fragpipe_run.threads,
                        update_folder_linux(tools_path),
                        python_arg,
                        log_path
                        ]
            output.append('{} --headless --workflow {} --manifest {} --workdir {} --ram {} --threads {} --config-tools-folder {}{} |& tee {}\n'.format(*arg_list))
        else:
            fragpipe_run.update_linux()
            # old style FragPipe (before 21.2-build41)
            arg_list = [fragpipe_run.fragpipe_path,
                        fragpipe_run.workflow_path,
                        fragpipe_run.manifest_path,
                        fragpipe_run.output_path,
                        fragpipe_run.ram,
                        fragpipe_run.threads,
                        fragpipe_run.msfragger_path,
                        fragpipe_run.philosopher_path,
                        fragpipe_run.ionquant_path,
                        ]
            if len(fragpipe_run.python_path) > 0:
                arg_list.append(fragpipe_run.python_path)
                arg_list.append(log_path)
                output.append('{} --headless --workflow {} --manifest {} --workdir {} --ram {} --threads {} --config-msfragger {} --config-philosopher {} --config-ionquant {} --config-python {} |& tee {}\n'.format(*arg_list))
            else:
                arg_list.append(log_path)
                output.append('{} --headless --workflow {} --manifest {} --workdir {} --ram {} --threads {} --config-msfragger {} --config-philosopher {} --config-ionquant {} |& tee {}\n'.format(*arg_list))

    if write_output:
        with open(batch_path, 'w', newline='') as outfile:
            for line in output:
                outfile.write(line)

    return output


def make_commands_windows(run_list, fragpipe_path, output_path):
    """
    NOT USED - test first
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
    run_list = parse_template(template_file, disable_list, fragpipe_path)
    output_dir = os.path.dirname(template_file)
    if write_to_linux:
        make_commands_linux(run_list, output_dir, NEW_FRAGPIPE)
    else:
        make_commands_windows(run_list, fragpipe_path, output_dir)


def update_folder_linux(folder_name):
    """
    helper to auto update windows directories to linux
    :param folder_name: path to update
    :return: updated path
    """
    # folder_name = folder_name.replace('\\\\', '\\')
    if '//corexfs' in folder_name:
        linux_name = folder_name.replace('//corexfs.med.umich.edu/proteomics', '/storage')
        linux_name = linux_name.replace('\\', '/')
    elif '\\corexfs' in folder_name:
        folder_name = folder_name.replace('\\', '/')
        while '//' in folder_name:
            folder_name = folder_name.replace('//', '/')
        linux_name = folder_name.replace('/corexfs.med.umich.edu/proteomics', '/storage')
    elif '=Z:' in folder_name:
        linux_name = folder_name.replace('Z:', '/storage')
        linux_name = linux_name.replace('\\', '/')
    elif 'Z:\\' in folder_name:
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


def update_folder_windows(folder_name):
    """
    helper to auto update linux directories to windows, correcting /storage to Z:
    :param folder_name:
    :return:
    """
    output = folder_name
    if "/storage" in folder_name:
        output = folder_name.replace('/storage', 'Z:')
    return output.replace('/', '\\')


def delete_old_temp_tools():
    """
    delete any previous runs' temp folders
    :return: void
    """
    pattern_str = r"{}_\d+".format(TEMP_TOOLS_NAME)
    temp_pattern = re.compile(pattern_str)
    for pathname in os.listdir(DEFAULT_TOOLS_PATH):
        if os.path.isdir(os.path.join(DEFAULT_TOOLS_PATH, pathname)) and temp_pattern.match(pathname):
            shutil.rmtree(os.path.join(DEFAULT_TOOLS_PATH, pathname))


if __name__ == '__main__':
    root = tkinter.Tk()
    root.withdraw()

    template = filedialog.askopenfilename(filetypes=[('FP Template', '.csv')])
    main(template, FRAGPIPE_PATH, USE_LINUX, TOOLS_TO_DISABLE)
    print('Done!')
