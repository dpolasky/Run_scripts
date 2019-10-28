"""
alternative method to prep fragger runs, using philosopher pipeline and easier handling of multiple raw directories/etc
"""

import PrepFraggerRuns
import tkinter
from tkinter import filedialog
import os
import shutil
from dataclasses import dataclass

# FRAGGER_JARNAME = 'msfragger-2.2-RC10_20191025.one-jar'
FRAGGER_JARNAME = 'msfragger-2.1_20191011.one-jar.jar'
# FRAGGER_JARNAME = 'msfragger-2.1_20191010_forceVarmod.one-jar.jar'

FRAGGER_MEM = 100
RAW_FORMAT = '.mzML'

# SHELL_TEMPLATE = r"\\corexfs.med.umich.edu\proteomics\dpolasky\tools\_SHELL_templates\base_open-offset.sh"
# SHELL_DIR = r"\\corexfs.med.umich.edu\proteomics\dpolasky\tools\_SHELL_templates"
# YML_DIR = r"\\corexfs.med.umich.edu\proteomics\dpolasky\tools\_YML_templates"


@dataclass
class RunContainer(object):
    """
    container for all run information needed to write the shell file
    """
    fragger_subfolder: str
    philosopher_folder: str
    param_file: str
    database_file: str
    shell_template: str
    raw_path: str
    yml_file: str
    fragger_path: str
    fragger_mem: int
    raw_format: str


def prepare_runs_yml(params_files, yml_files, raw_path_files, shell_template, main_dir):
    """
    Generates subfolders for each Fragger .params file found in the provided outer directory. Uses
    params file name as a the subfolder name
    :param params_files: list of parameter files to use
    :param yml_files: list of config file with philosopher params
    :param raw_path_files: list of filenames for .pathraw files
    :param shell_template: single .sh file (full path) to use as template for output shell script
    :param main_dir: top directory in which to make output subdirectories
    :return: void
    """
    run_containers = []
    for raw_path in raw_path_files:
        for yml_file in yml_files:
            # For each param file, make a subfolder in each raw subfolder to run that analysis
            for params_file in params_files:
                run_container = generate_single_run(params_file, yml_file, raw_path, shell_template, main_dir)
                run_containers.append(run_container)
    return run_containers


def generate_single_run(param_file, yml_file, raw_path, shell_template, main_dir):
    """
    Generate a RunContainer from the provided information and return it
    :param param_file: .params file for Fragger (path)
    :param yml_file: .yml file for Philosopher (path)
    :param raw_path: directory in which to find raw data (path)
    :param main_dir: where to save results
    :param shell_template: single .sh file (full path) to use as template for output shell script
    :return: RunContainer
    :rtype: RunContainer
    """
    # raw_folder = os.path.join(main_dir, os.path.basename(raw_path))
    # if not os.path.exists(raw_folder):
    #     os.makedirs(raw_folder)
    run_folder = os.path.join(main_dir, '__FraggerResults')
    if not os.path.exists(run_folder):
        os.makedirs(run_folder)

    param_name = os.path.basename(os.path.splitext(param_file)[0])
    param_subfolder = os.path.join(run_folder, param_name)
    params_filename = param_name + '.params'
    if not os.path.exists(param_subfolder):
        os.makedirs(param_subfolder)

    # get database file
    db_file = PrepFraggerRuns.get_db_file(param_file)

    # move params file to new folder
    # shutil.move(params_file, os.path.join(subdir_path, params_filename))
    shutil.copy(param_file, os.path.join(param_subfolder, params_filename))

    # copy yml file for philosopher and edit the fasta path in it
    yml_output_path = os.path.join(param_subfolder, os.path.basename(yml_file))
    shutil.copy(yml_file, yml_output_path)
    edit_yml(yml_output_path, db_file)
    yml_final_linux_path = PrepFraggerRuns.update_folder_linux(yml_output_path)

    # generate single shell for individual runs (if desired)
    run_container = RunContainer(param_subfolder, param_file, db_file, shell_template, raw_path, yml_final_linux_path,
                                 FRAGGER_JARNAME, FRAGGER_MEM, RAW_FORMAT)
    gen_single_shell_container(run_container, write_output=True)
    return run_container


def gen_multilevel_shell(run_containers, main_dir):
    """
    Generate a shell script to navigate the generated file structure and run all requested analyses
    :param run_containers: list of run containers to generate a shell script to run all of
    :type run_containers: list[RunContainer]
    :param main_dir: directory in which to save output
    :return: void
    """
    output_shell_name = os.path.join(main_dir, 'fragger_shell_multi.sh')
    with open(output_shell_name, 'w', newline='') as shellfile:
        # header
        shellfile.write('#!/bin/bash\nset -xe\n\n')
        for index, run_container in enumerate(run_containers):
            shellfile.write('# Run {} of {}*************************************\n'.format(index + 1, len(run_containers)))
            run_shell_lines = gen_single_shell_container(run_container, write_output=False)
            for line in run_shell_lines:
                shellfile.write(line)
            shellfile.write('\n')


def edit_yml(yml_file, database_path):
    """
    Edit the yml file to include the fasta database path
    :param yml_file: path string
    :param database_path: path string
    :return: void
    """
    lines = []
    with open(yml_file, 'r') as readfile:
        for line in list(readfile):
            if line.startswith('  protein_database'):
                line = '  protein_database: {}\n'.format(database_path)
            lines.append(line)
    with open(yml_file, 'w') as outfile:
        for line in lines:
            outfile.write(line)


def get_raw_folder(pathraw_file):
    """
    Get the specified raw dir from a pathraw file (text with nothing but the raw linux path)
    :param pathraw_file: template file
    :return: raw file path string
    """
    raw_name = os.path.basename(os.path.splitext(pathraw_file)[0])
    with open(pathraw_file, 'r') as readfile:
        line = readfile.readline()
        if line.endswith('\n'):
            line = line.rstrip('\n')
        return line, raw_name


def gen_single_shell_container(run_container: RunContainer, write_output):
    """
    Wrapper method to call gen_single_shell but from a RunContainer
    :param run_container: run container
    :param write_output: whether to save the output directly to file or only return it
    :return: list of lines
    """
    output = PrepFraggerRuns.gen_single_shell(params_file=run_container.param_file,
                                              db_file=run_container.database_file,
                                              subfolder=run_container.subfolder,
                                              shell_template=run_container.shell_template,
                                              individ_folder=None,
                                              fragger_jar=run_container.fragger_path,
                                              fragger_mem=run_container.fragger_mem,
                                              raw_fmt=run_container.raw_format,
                                              write_output=write_output,
                                              yml_file=run_container.yml_file,
                                              raw_dir=run_container.raw_path)
    return output


# def main(batch_mode):
#     """
#     Batch mode to generate multiple runs all in one script
#     :param batch_mode: bool
#     :return: void
#     """
#     run_batch = True
#     all_runs = []
#     maindir = ''
#     while run_batch:
#         paramfiles = filedialog.askopenfilenames(filetypes=[('.params', '.params')])
#         maindir = os.path.dirname(paramfiles[0])
#
#         ymlfiles = filedialog.askopenfilenames(initialdir=YML_DIR, filetypes=[('YML', '.yml')])
#         base_templates = filedialog.askopenfilename(initialdir=os.path.dirname(SHELL_DIR),
#                                                     filetypes=[('shell files', '.sh')])
#         filedialog.askopenfilename(initialdir=maindir)
#
#         current_runs = prepare_runs_yml(paramfiles, ymlfiles, base_templates, base_templates, maindir)
#         all_runs.extend(current_runs)
#
#         if batch_mode:
#             continue_bool = simpledialog.messagebox.askyesno('Continue?', 'Continue entering?')
#             if not continue_bool:
#                 run_batch = False
#         else:
#             run_batch = False
#
#     if len(all_runs) > 1:
#         gen_multilevel_shell(all_runs, maindir)


def batch_template_run():
    """
    Select and load template file(s) to be run
    :return: void
    """
    templates = filedialog.askopenfilenames(filetypes=[('Templates', '.csv')])
    # Get set(s) of runs from each template and generate a multilevel shell for each
    for template in templates:
        template_run_list, main_dir = parse_template(template)
        if len(template_run_list) > 1:
            gen_multilevel_shell(template_run_list, main_dir)


def parse_template(template_file):
    """
    Read a template into a list of run containers to be run together (or several lists if multiple provided)
    :param template_file: path to template csv
    :return: list of list of RunContainers
    """
    run_list = []
    with open(template_file, 'r') as tempfile:
        for line in list(tempfile):
            if line.startswith('#'):
                continue
            splits = line.rstrip('\n').split(',')
            if line.startswith('!'):
                # This is the maindir line - start a new list here
                current_maindir = splits[1]
            elif line is not '\n':
                # add new analysis to the current
                param_path = splits[0]
                # param_path = os.path.join(current_maindir, splits[0])
                if not param_path.endswith('.params'):
                    param_path += '.params'
                # yml_path = PrepFraggerRuns.update_folder_linux(splits[1])     # this is done later
                raw_path = PrepFraggerRuns.update_folder_linux(splits[3])
                run_container_list = prepare_runs_yml(params_files=[param_path], yml_files=[splits[1]], raw_path_files=[raw_path], shell_template=splits[2], main_dir=current_maindir)
                run_list.extend(run_container_list)
    return run_list, current_maindir


if __name__ == '__main__':
    root = tkinter.Tk()
    root.withdraw()

    # main(batch_mode=False)
    # main(batch_mode=True)
    batch_template_run()
