"""
alternative method to prep fragger runs, using philosopher pipeline and easier handling of multiple raw directories/etc
"""

import PrepFraggerRuns
import tkinter
from tkinter import filedialog
import os
import shutil
from dataclasses import dataclass
import EditParamsByActivation

FRAGGER_JARNAME = 'msfragger-2.2-RC10_20191031.one-jar.jar'
# FRAGGER_JARNAME = 'msfragger-2.1_20191011.one-jar.jar'
# FRAGGER_JARNAME = 'msfragger-2.1_20191010_forceVarmod.one-jar.jar'

FRAGGER_MEM = 200
RAW_FORMAT = '.mzML'
# RAW_FORMAT = '.d'

# SHELL_TEMPLATE = r"\\corexfs.med.umich.edu\proteomics\dpolasky\tools\_SHELL_templates\base_open-offset.sh"
# SHELL_DIR = r"\\corexfs.med.umich.edu\proteomics\dpolasky\tools\_SHELL_templates"
# YML_DIR = r"\\corexfs.med.umich.edu\proteomics\dpolasky\tools\_YML_templates"


@dataclass
class RunContainer(object):
    """
    container for all run information needed to write the shell file
    """
    subfolder: str
    param_file: str
    database_file: str
    shell_template: str
    raw_path: str
    yml_file: str
    fragger_path: str
    fragger_mem: int
    raw_format: str
    activation_type: str


def prepare_runs_yml(params_files, yml_files, raw_path_files, shell_template, main_dir, activation_types):
    """
    Generates subfolders for each Fragger .params file found in the provided outer directory. Uses
    params file name as a the subfolder name
    :param params_files: list of parameter files to use
    :param yml_files: list of config file with philosopher params
    :param raw_path_files: list of filenames for .pathraw files
    :param shell_template: single .sh file (full path) to use as template for output shell script
    :param main_dir: top directory in which to make output subdirectories
    :param activation_types: list of strings (standardized 'HCD', 'AIETD', etc)
    :return: void
    """
    run_containers = []
    for raw_path in raw_path_files:
        for yml_file in yml_files:
            # For each param file, make a subfolder in each raw subfolder to run that analysis
            for params_file in params_files:
                if len(activation_types) > 0:
                    for activation_type in activation_types:
                        run_container = generate_single_run(params_file, yml_file, raw_path, shell_template, main_dir, activation_type)
                        run_containers.append(run_container)
                else:
                    run_container = generate_single_run(params_file, yml_file, raw_path, shell_template, main_dir)
                    run_containers.append(run_container)
    return run_containers


def generate_single_run(base_param_path, yml_file, raw_path, shell_template, main_dir, activation_type=None):
    """
    Generate a RunContainer from the provided information and return it
    :param base_param_path: .params file for Fragger (path)
    :param yml_file: .yml file for Philosopher (path)
    :param raw_path: directory in which to find raw data (path)
    :param main_dir: where to save results
    :param shell_template: single .sh file (full path) to use as template for output shell script
    :param activation_type: string ('HCD', etc)
    :return: RunContainer
    :rtype: RunContainer
    """
    run_folder = os.path.join(main_dir, '__FraggerResults')
    if not os.path.exists(run_folder):
        os.makedirs(run_folder)

    param_name = os.path.basename(os.path.splitext(base_param_path)[0])
    param_subfolder = os.path.join(run_folder, param_name)
    params_filename = param_name + '.params'
    if not os.path.exists(param_subfolder):
        os.makedirs(param_subfolder)

    if activation_type is not '':
        param_path = EditParamsByActivation.create_param_file(base_param_path, activation_type, output_dir=param_subfolder)
    else:
        param_path = os.path.join(param_subfolder, params_filename)
        shutil.copy(base_param_path, param_path)
        activation_type = ''

    # get database file
    db_file = PrepFraggerRuns.get_db_file(base_param_path)

    # copy yml file for philosopher and edit the fasta path in it
    yml_output_path = os.path.join(param_subfolder, 'phil_config.yml')
    shutil.copy(yml_file, yml_output_path)
    edit_yml(yml_output_path, db_file)
    yml_final_linux_path = PrepFraggerRuns.update_folder_linux(yml_output_path)

    # generate single shell for individual runs (if desired)
    run_container = RunContainer(param_subfolder, param_path, db_file, shell_template, raw_path, yml_final_linux_path,
                                 FRAGGER_JARNAME, FRAGGER_MEM, RAW_FORMAT, activation_type)
    gen_single_shell_activation(run_container, write_output=True, run_philosopher=True)
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
        all_subfolders = []
        shell_base = ''
        for index, run_container in enumerate(run_containers):
            shellfile.write('# Fragger Run {} of {}*************************************\n'.format(index + 1, len(run_containers)))
            run_shell_lines = gen_single_shell_activation(run_container, write_output=False, run_philosopher=False)
            shell_base = PrepFraggerRuns.read_shell(run_container.shell_template)
            for line in run_shell_lines:
                shellfile.write(line)
            shellfile.write('\n')

            # add philosopher run on all subfolders at the end
            if run_container.subfolder not in all_subfolders:
                all_subfolders.append(run_container.subfolder)

        # run philosopher
        shellfile.write('#********************Philosopher Runs*******************\n')
        for index, subfolder in enumerate(all_subfolders):
            shellfile.write('# Philosopher {} of {}*************************************\n'.format(index + 1, len(all_subfolders)))
            shellfile.write('cd {}\n'.format(PrepFraggerRuns.update_folder_linux(subfolder)))
            phil_lines = gen_philosopher_lines(shell_base)
            for line in phil_lines:
                shellfile.write(line)
            shellfile.write('\n')


def gen_philosopher_lines(shell_template_lines):
    """
    Generate philosopher instructions for a provided subfolder and shell template
    :param shell_template_lines: lines from standard template passed to run container
    :return: list of strings to append to file
    """
    output = []
    for line in shell_template_lines:
        if line.startswith('toolDirPath') or line.startswith('philosopherPath') or line.startswith('$philosopherPath'):
            if line.startswith('$philosopherPath pipeline'):
                line = '$philosopherPath pipeline --config {} ./\n'.format('phil_config.yml')
                output.append(line)
            else:
                output.append(line)
        if line.startswith('analysisName') or line.startswith('cp '):
            output.append(line)
    return output


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


def gen_single_shell_activation(run_container: RunContainer, write_output, run_philosopher):
    """
    Wrapper method to call gen_single_shell but from a RunContainer
    :param run_container: run container
    :param write_output: whether to save the output directly to file or only return it
    :param run_philosopher: skip all philosopher lines if false
    :return: list of lines
    """
    new_shell_name = os.path.join(run_container.subfolder, 'fragger_shell.sh')
    shell_lines = PrepFraggerRuns.read_shell(run_container.shell_template)
    output = []

    # add a 'cd' to allow pasting together
    linux_folder = PrepFraggerRuns.update_folder_linux(run_container.subfolder)
    if write_output:
        output.append('#!/bin/bash\nset -xe\n\n')
    output.append('# Change dir to local workspace\ncd {}\n'.format(linux_folder))

    for line in shell_lines:
        if line.startswith('fasta'):
            output.append('fastaPath="{}"\n'.format(run_container.database_file))
        elif line.startswith('fraggerParams'):
            output.append('fraggerParamsPath="./{}"\n'.format(os.path.basename(run_container.param_file)))
        elif line.startswith('dataDirPath'):
            output.append('dataDirPath="{}"\n'.format(run_container.raw_path))
        elif line.startswith('msfraggerPath'):
            output.append('msfraggerPath=$toolDirPath/{}\n'.format(run_container.fragger_path))
        elif line.startswith('java'):
            if run_container.activation_type is '':
                output.append('java -Xmx{}G -jar $msfraggerPath $fraggerParamsPath $dataDirPath/*{}\n'.format(run_container.fragger_mem, run_container.raw_format))
            else:
                output.append('java -Xmx{}G -jar $msfraggerPath $fraggerParamsPath $dataDirPath/*_{}{}\n'.format(run_container.fragger_mem, run_container.activation_type, run_container.raw_format))
        elif line.startswith('$philosopherPath pipeline'):
            if run_philosopher:
                output.append('$philosopherPath pipeline --config {} ./\n'.format(run_container.yml_file))
        elif line.startswith('$philosopherPath'):
            if run_philosopher:
                output.append(line)
        elif line.startswith('analysisName') or line.startswith('cp '):
            if run_philosopher:
                output.append(line)
        else:
            output.append(line)

    if write_output:
        with open(new_shell_name, 'w', newline='') as shellfile:
            for line in output:
                shellfile.write(line)
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
                if current_maindir is '':
                    current_maindir = splits[2]
                    if current_maindir is '':
                        raise ValueError('Main dir was not read on line {}, breaking'.format(line))
            elif line is not '\n':
                # add new analysis to the current
                param_path = splits[0]
                # param_path = os.path.join(current_maindir, splits[0])
                if not param_path.endswith('.params'):
                    param_path += '.params'
                # yml_path = PrepFraggerRuns.update_folder_linux(splits[1])     # this is done later
                raw_path = PrepFraggerRuns.update_folder_linux(splits[4])
                activation_types = splits[1].split(';')
                run_container_list = prepare_runs_yml(params_files=[param_path], yml_files=[splits[2]], raw_path_files=[raw_path], shell_template=splits[3], main_dir=current_maindir, activation_types=activation_types)
                run_list.extend(run_container_list)
    return run_list, current_maindir


if __name__ == '__main__':
    root = tkinter.Tk()
    root.withdraw()

    # main(batch_mode=False)
    # main(batch_mode=True)
    batch_template_run()
