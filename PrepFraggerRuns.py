"""
Module to do gruntwork for preparing many fragger runs in various folders
"""

import tkinter
from tkinter import filedialog
import os
import shutil
import PrepIndividFDRruns

PREP_INDIVID_TOO = False
INDIVID_SHELL = r"Z:\dpolasky\tools\Philosopher_shells\philosopher_shell_individ.sh"      # mass width 4000, open search
# INDIVID_SHELL = r"Z:\dpolasky\tools\Philosopher_shells\philosopher_shell_individ_masswid1000.sh"
# INDIVID_SHELL = r"Z:\dpolasky\tools\Philosopher_shells\philosopher_shell_individ_CLOSED.sh"


def prepare_runs(params_files, shell_template):
    """
    Generates subfolders for each Fragger .params file found in the provided outer directory. In each subfolder,
    includes the database file (if found) and shell script (if found), edited to match the params file. Uses
    params file name as a the subfolder name
    :param params_files: list of parameter files to use
    :param shell_template: shell template to use
    :return: void
    """
    # params_files = [os.path.join(outer_dir, x) for x in os.listdir(outer_dir) if x.endswith('.params')]
    subfolders = []
    individ_folders = []
    db_file = ''

    for params_file in params_files:
        subdir_name = os.path.splitext(params_file)[0]
        params_filename = os.path.basename(subdir_name) + '.params'
        if not os.path.exists(subdir_name):
            os.makedirs(subdir_name)

        db_file = get_db_file(params_file)

        # move params file to new folder
        shutil.move(params_file, os.path.join(subdir_name, params_filename))

        # copy database file
        # shutil.copy(db_file, subdir_name)

        # also generate the individual FDR folder if requested
        if PREP_INDIVID_TOO:
            individ_folder = PrepIndividFDRruns.prep_individ_run(subdir_name, INDIVID_SHELL)
            individ_folder = update_folder_linux(individ_folder)
            individ_folders.append(individ_folder)
            gen_single_shell(params_file, db_file, subdir_name, shell_template, individ_folder)
        else:
            # generate single shell for individual runs (if desired)
            gen_single_shell(params_file, db_file, subdir_name, shell_template)

        # change subdir name for linux shell navigation if needed
        subdir_name = update_folder_linux(subdir_name)
        subfolders.append(subdir_name)

    # generate shell script from template if multiple runs are present
    if len(params_files) > 1:
        gen_multirun_shell(params_files, db_file, subfolders, shell_template, individ_folders)


def get_db_file(param_file):
    """
    Read the database path from the parameters file (relative path as listed in params file).
    That way can run with various DBs and paths without having to manually change everything
    NOTE: relative path must be relative to the base directory
    :param param_file: path to param file
    :return: DB file relative path string to write to files
    """
    with open(param_file, 'r') as readfile:
        for line in list(readfile):
            if line.startswith('database_name'):
                splits = line.rstrip('\n').split('=')
                db_file = splits[1].strip()
                return db_file


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
    else:
        linux_name = folder_name
    return linux_name


def gen_single_shell(params_file, db_file, subfolder, shell_template, individ_folder=None, fragger_jar=None,
                     fragger_mem=None, raw_fmt=None, write_output=True, yml_file=None, raw_dir=None):
    """
    Generate a single shell file for running just this parameter file
    :param params_file:
    :param db_file:
    :param subfolder:
    :param shell_template:
    :param individ_folder:
    :param fragger_jar: if provided, edit this line. MUST BE FILE NAME ONLY, NOT PATH
    :param fragger_mem: if provided, edit to include
    :param raw_fmt: string, default '.mzML' if provided
    :param write_output: if true, writes output directly
    :param yml_file: if provided, edit this line. MUST BE FILE NAME ONLY, NOT PATH
    :param raw_dir: full path to raw files
    :return: list of string outputs for this shell file
    """
    new_shell_name = os.path.join(subfolder, 'fragger_shell.sh')
    shell_lines = read_shell(shell_template)
    output = []

    # add a 'cd' to allow pasting together
    linux_folder = update_folder_linux(subfolder)
    if write_output:
        output.append('#!/bin/bash\nset -xe\n\n')
    output.append('# Change dir to local workspace\ncd {}\n'.format(linux_folder))

    for line in shell_lines:
        if line.startswith('fasta'):
            output.append('fastaPath="{}"\n'.format(db_file))
        elif line.startswith('fraggerParams'):
            output.append('fraggerParamsPath="./{}"\n'.format(os.path.basename(params_file)))
        elif line.startswith('dataDirPath'):
            if raw_dir is not None:
                output.append('dataDirPath="{}"\n'.format(raw_dir))
        elif line.startswith('msfraggerPath'):
            if fragger_jar is not None:
                output.append('msfraggerPath=$toolDirPath/{}\n'.format(fragger_jar))
        elif line.startswith('java'):
            if fragger_mem is not None:
                output.append('java -Xmx{}G -jar $msfraggerPath $fraggerParamsPath $dataDirPath/*{}\n'.format(fragger_mem, raw_fmt))
        elif line.startswith('$philosopherPath pipeline'):
            if yml_file is not None:
                output.append('$philosopherPath pipeline --config {} ./\n'.format(yml_file))
        else:
            output.append(line)

    if PREP_INDIVID_TOO:
        output.append('# Individual FDR run ***********************\n')
        output.append('cd {}\n'.format(individ_folder))
        output.append('fastaPath="../{}"\n'.format(db_file))
        individ_header, individ_mainlines = parse_shell_template(INDIVID_SHELL)
        for line in individ_mainlines:
            output.append(line)
        output.append('\n')

    if write_output:
        with open(new_shell_name, 'w', newline='') as shellfile:
            for line in output:
                shellfile.write(line)
    return output


def gen_multirun_shell(params_files, db_file, subfolders, shell_template, individ_fdr_folders):
    """
    Generate a single shell script for running all the parameter files as individual Fragger runs
    :param params_files:
    :param db_file:
    :param subfolders:
    :param shell_template:
    :param individ_fdr_folders:
    :return:
    """
    new_shell_name = os.path.join(os.path.dirname(shell_template), 'fragger_shell_multi.sh')
    shell_header, shell_mainlines = parse_shell_template(shell_template)
    individ_header, individ_mainlines = parse_shell_template(INDIVID_SHELL)

    with open(new_shell_name, 'w', newline='') as shellfile:
        # write header to the start of the file
        for line in shell_header:
            shellfile.write(line)

        for index, params_file in enumerate(params_files):
            # write the new fasta path and parameter file path
            shellfile.write('# RUN {} **************************************\n'.format(index + 1))
            current_dirname = subfolders[index].replace('\\', '/')
            shellfile.write('cd {}\n'.format(current_dirname))
            shellfile.write('fastaPath="{}"\n'.format(db_file))
            shellfile.write('fraggerParamsPath="./{}"\n'.format(os.path.basename(params_file)))

            # add the remaining run lines from the template
            for line in shell_mainlines:
                shellfile.write(line)

            # add individual FDR run if requested
            if len(individ_fdr_folders) > 0:
                shellfile.write('# Individual FDR run ***********************\n')
                shellfile.write('cd {}\n'.format(individ_fdr_folders[index]))
                shellfile.write('fastaPath="../{}"\n'.format(db_file))
                # write entire individual shell template
                for line in individ_mainlines:
                    shellfile.write(line)
                shellfile.write('\n\n')


def parse_shell_template(shell_template):
    """
    Get the header (tool paths) and run lines from a template shell file to set up multiple runs
    :param shell_template:
    :return: header (list of strings), run lines (list of strings)
    """
    header = []
    run_lines = []
    header_flag = True
    with open(shell_template, 'r') as shfile:
        for line in list(shfile):
            if header_flag:
                # still in the header. Check for final header line
                header.append(line)
                if line.startswith('decoy'):
                    header_flag = False
            else:
                run_lines.append(line)
    return header, run_lines


def read_shell(shell_file):
    """
    read file
    :param shell_file:
    :return:
    """
    with open(shell_file, 'r') as myfile:
        lines = list(myfile)
    return lines


if __name__ == '__main__':
    root = tkinter.Tk()
    root.withdraw()

    # maindir = filedialog.askdirectory()
    shell_base = filedialog.askopenfilename(filetypes=[('.sh', '.sh')])
    maindir = os.path.dirname(shell_base)

    paramfiles = filedialog.askopenfilenames(filetypes=[('.params', '.params')])

    # try to find DB file and shell template
    dirfiles = os.listdir(maindir)
    # shell_files = [os.path.join(maindir, x) for x in dirfiles if x.endswith('.sh')]
    # db_files = [os.path.join(maindir, x) for x in dirfiles if x.endswith('.fasta') or x.endswith('.fas')]
    prepare_runs(paramfiles, shell_base)

    # if len(db_files) == 1:
    #     dbfile = db_files[0]
    #     prepare_runs(paramfiles, dbfile, shell_base)
    # else:
    #     print('No DB files or multiple DB files, not sure which to use. Exiting')
