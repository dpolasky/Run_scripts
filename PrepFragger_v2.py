"""
alternative method to prep fragger runs, using philosopher pipeline and easier handling of multiple raw directories/etc
"""

import PrepFraggerRuns
import tkinter
from tkinter import filedialog
import os
import shutil
import subprocess
from dataclasses import dataclass
import EditParams

# FRAGGER_JARNAME = 'msfragger-2.3-RC3_20191120_varmodGlycSequon.one-jar.jar'
# FRAGGER_JARNAME = 'msfragger-2.3-RC9_20191210_noVarmodDelete.one-jar.jar'
# FRAGGER_JARNAME = 'msfragger-2.3-RC11_20191223_flexY.one-jar.jar'
# FRAGGER_JARNAME = 'msfragger-2.4-RC3_20200220_glycCalOffsets.one-jar.jar'
# FRAGGER_JARNAME = 'msfragger-2.4-RC4_Glyco-1.0_20200301-calOffsets.one-jar.jar'   # same as 2/28 full fix, except WITH cal offsets

# FRAGGER_JARNAME = 'msfragger-2.4-RC1_20200203.one-jar.jar'
# FRAGGER_JARNAME = 'msfragger-2.4-RC4_Glyco-1.0_20200228-fix-full.one-jar.jar'       # correct glyco1.0, no cal offsets
# FRAGGER_JARNAME = 'msfragger-2.4-RC4_Glyco-1.0_20200303-noRebaseWithFixes-noCalOffset.one-jar.jar'
# FRAGGER_JARNAME = 'msfragger-2.4-RC4_Glyco-1.0_20200316.one-jar.jar'
# FRAGGER_JARNAME = 'msfragger-2.4-RC6_Glyco-1.0_20200320_intFilterFix.one-jar.jar'

# FRAGGER_JARNAME = 'msfragger-2.4_20200409_noMerge-intGreater.one-jar.jar'
# FRAGGER_JARNAME = 'msfragger-2.4_20200413_fixCrash-debugRemoved.one-jar.jar'
# FRAGGER_JARNAME = 'msfragger-2.4_20200504_noShiftby18.one-jar.jar'
FRAGGER_JARNAME = 'msfragger-2.5-rc5_20200525.one-jar.jar'

FRAGGER_MEM = 400
RAW_FORMAT = '.mzML'
# RAW_FORMAT = '.d'

QUANT_COPY_ANNOTATION_FILE = True

# OVERRIDE_MAINDIR = False
OVERRIDE_MAINDIR = True    # default True. If false, will read maindir from file rather than using the param path (use false for combined runs)

SERIAL_PHILOSOPHER = False
# SERIAL_PHILOSOPHER = True      # set True if using very large data (e.g. 10M or more PSMs), as Philosopher will use too much memory and crash if multithreaded

RUN_IN_PROGRESS = ''  # to avoid overwriting multi.sh
# RUN_IN_PROGRESS = '2'     # NOTE - DO NOT RUN MULTIPLE SEARCHES ON THE SAME RAW DATA AT THE SAME TIME (should be obvious, but can be forgotten...)

# REMOVE_LOCALIZE_DELTAMASS = True
REMOVE_LOCALIZE_DELTAMASS = False   # default

SPLIT_DBS = 0
# SPLIT_DBS = 7       # set > 0 if using split database
SPLIT_PYTHON_PATH = '/storage/teog/anaconda3/bin/python3'  # linux path since this just gets written directly to the shell script
SPLIT_DB_SCRIPT = '/storage/dpolasky/tools/msfragger_pep_split_20191106.py'


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
    enzyme_subfolder: str
    enzyme: str
    enzyme_yml_path: str
    split_dbs: int      # for splitting database if > 0


def prepare_runs_yml(params_files, yml_files, raw_path_files, shell_template, main_dir, activation_types, enzymes, raw_names_list, fragger_jar, annotation_path):
    """
    Generates subfolders for each Fragger .params file found in the provided outer directory. Uses
    params file name as a the subfolder name
    :param params_files: list of parameter files to use
    :param yml_files: list of config file with philosopher params
    :param raw_path_files: list of directories containing raw files
    :param shell_template: single .sh file (full path) to use as template for output shell script
    :param main_dir: top directory in which to make output subdirectories
    :param activation_types: list of strings (standardized 'HCD', 'AIETD', etc)
    :param enzymes: list of enzyme strings (or '')
    :param raw_names_list: list of raw names (only required if >1 raw path provided)
    :param fragger_jar: name of the fragger jar file (including .jar). NOT including path since that's appended from ToolDir in the shell script
    :return: void
    """
    run_containers = []
    for index, raw_path in enumerate(raw_path_files):
        # handle raw names to append
        try:
            raw_path_append = raw_names_list[index]
        except IndexError:
            if index == 0:
                # no problem - single raw specified so no appending needed
                raw_path_append = ''
            else:
                # more raw directories provided than names, which means a file would be overwritten
                print('ERROR: more raw paths provided than names; skipping')
                return []

        for yml_file in yml_files:
            # For each param file, make a subfolder in each raw subfolder to run that analysis
            for params_file in params_files:
                if len(enzymes) > 1:
                    # multi-enzyme run
                    for enzyme in enzymes:
                        if len(activation_types) > 0:
                            for activation_type in activation_types:
                                run_container = generate_single_run(params_file, yml_file, raw_path, shell_template, main_dir, fragger_jar, activation_type=activation_type, enzyme=enzyme, raw_name_append=raw_path_append, annotation_path=annotation_path)
                                run_containers.append(run_container)
                        else:
                            run_container = generate_single_run(params_file, yml_file, raw_path, shell_template, main_dir, fragger_jar, enzyme=enzyme, raw_name_append=raw_path_append, annotation_path=annotation_path)
                            run_containers.append(run_container)
                else:
                    # single enzyme run
                    if len(activation_types) > 0:
                        for activation_type in activation_types:
                            run_container = generate_single_run(params_file, yml_file, raw_path, shell_template, main_dir, fragger_jar, activation_type=activation_type, raw_name_append=raw_path_append, annotation_path=annotation_path)
                            run_containers.append(run_container)
                    else:
                        run_container = generate_single_run(params_file, yml_file, raw_path, shell_template, main_dir, fragger_jar, raw_name_append=raw_path_append, annotation_path=annotation_path)
                        run_containers.append(run_container)
    return run_containers


def generate_single_run(base_param_path, yml_file, raw_path, shell_template, main_dir, fragger_jar, activation_type=None, enzyme=None, raw_name_append='', annotation_path=''):
    """
    Generate a RunContainer from the provided information and return it
    :param base_param_path: .params file for Fragger (path)
    :param yml_file: .yml file for Philosopher (path)
    :param raw_path: directory in which to find raw data (path)
    :param main_dir: where to save results
    :param shell_template: single .sh file (full path) to use as template for output shell script
    :param activation_type: string ('HCD', etc)
    :param enzyme: string ('TRYP', etc)
    :param raw_name_append: if provided, append this name to output folder to distinguish between runs of same params on different raw data
    :param fragger_jar: name of the fragger jar file (including .jar). NOT including path since that's appended from ToolDir in the shell script
    :return: RunContainer
    :rtype: RunContainer
    """
    run_folder = os.path.join(main_dir, '__FraggerResults')
    if not os.path.exists(run_folder):
        os.makedirs(run_folder)

    param_name = os.path.basename(os.path.splitext(base_param_path)[0])
    if raw_name_append is not '':
        combined_name = '{}_{}'.format(param_name, raw_name_append)
    else:
        combined_name = param_name
    param_subfolder = os.path.join(run_folder, combined_name)
    params_filename = param_name + '.params'
    if not os.path.exists(param_subfolder):
        os.makedirs(param_subfolder)

    if enzyme is not None:
        enzyme_subfolder = os.path.join(param_subfolder, enzyme)
        if not os.path.exists(enzyme_subfolder):
            os.makedirs(enzyme_subfolder)

        if activation_type is not '':
            param_path = EditParams.create_param_file(base_param_path, activation_type=activation_type, output_dir=enzyme_subfolder, enzyme=enzyme, remove_localize_delta_mass=REMOVE_LOCALIZE_DELTAMASS)
        else:
            param_path = EditParams.create_param_file(base_param_path, activation_type=None, output_dir=enzyme_subfolder, enzyme=enzyme, remove_localize_delta_mass=REMOVE_LOCALIZE_DELTAMASS)

    else:
        # single enzyme run
        if not activation_type == '':
            param_path = EditParams.create_param_file(base_param_path, activation_type=activation_type, output_dir=param_subfolder, remove_localize_delta_mass=REMOVE_LOCALIZE_DELTAMASS)
        else:
            param_path = EditParams.create_param_file(base_param_path, activation_type=None, output_dir=param_subfolder, remove_localize_delta_mass=REMOVE_LOCALIZE_DELTAMASS)
            # param_path = os.path.join(param_subfolder, params_filename)
            # shutil.copy(base_param_path, param_path)
        enzyme_subfolder = ''

    # get database file
    db_file = PrepFraggerRuns.get_db_file(base_param_path)

    if enzyme is not None:
        # make 2 ymls - one for peptide prophet only and one for the main philosopher run
        enzyme_yml = make_yml_peptideproph_only(yml_file, '../{}'.format(db_file), enzyme, enzyme_subfolder)
        enzyme_yml_linux = PrepFraggerRuns.update_folder_linux(enzyme_yml)

        yml_output_path = os.path.join(param_subfolder, 'phil_config.yml')
        shutil.copy(yml_file, yml_output_path)
        edit_yml(yml_output_path, db_file, disable_peptide_prophet=True)
        if annotation_path is not '':
            shutil.copy(annotation_path, os.path.join(enzyme_subfolder, os.path.basename(annotation_path)))
        symlink_raw_files(enzyme_subfolder, raw_path, RAW_FORMAT, activation_type, enzyme)
        yml_final_linux_path = PrepFraggerRuns.update_folder_linux(yml_output_path)
        run_container = RunContainer(param_subfolder, param_path, db_file, shell_template, raw_path,
                                     yml_final_linux_path, fragger_jar, FRAGGER_MEM, RAW_FORMAT, activation_type,
                                     enzyme_subfolder, enzyme, enzyme_yml_linux, SPLIT_DBS)
        gen_single_shell_activation(run_container, write_output=True, run_philosopher=False)

    else:
        # copy yml file for philosopher and edit the fasta path in it
        yml_output_path = os.path.join(param_subfolder, 'phil_config.yml')
        shutil.copy(yml_file, yml_output_path)
        edit_yml(yml_output_path, db_file)
        if annotation_path is not '':
            shutil.copy(annotation_path, os.path.join(param_subfolder, os.path.basename(annotation_path)))
        # symlink raw files for downstream tools
        symlink_raw_files(param_subfolder, raw_path, RAW_FORMAT, activation_type, '')
        yml_final_linux_path = PrepFraggerRuns.update_folder_linux(yml_output_path)

        # generate single shell for individual runs (if desired)
        run_container = RunContainer(param_subfolder, param_path, db_file, shell_template, raw_path, yml_final_linux_path,
                                     fragger_jar, FRAGGER_MEM, RAW_FORMAT, activation_type, '', '', '', SPLIT_DBS)
        gen_single_shell_activation(run_container, write_output=True, run_philosopher=True)
    return run_container


def symlink_raw_files(param_subfolder, raw_path, raw_format, activation_type, enzyme):
    """
    Create symbolic links for all specified raw files in the raw path to the results folder for downstream
    tools. Supports only creating for particular activation type/enzyme as needed
    :param param_subfolder: folder path in which to link
    :type param_subfolder: str
    :param raw_path: path to folder of raw data files (linux path)
    :type raw_path: str
    :param activation_type: str or ''
    :type activation_type: str
    :param raw_format: file extension (e.g. '.mzML')
    :type raw_format: str
    :param enzyme: str or ''
    :type enzyme: str
    :return: void
    :rtype:
    """
    # Get all raw files
    if enzyme is not '':
        if activation_type is not '':
            contain_strs = ['_{}'.format(activation_type), '_{}'.format(enzyme), raw_format]
        else:
            contain_strs = ['_{}'.format(enzyme), raw_format]
    else:
        if activation_type is not '':
            contain_strs = ['_{}'.format(activation_type), raw_format]
        else:
            contain_strs = [raw_format]

    windows_raw_path = PrepFraggerRuns.update_folder_windows(raw_path)
    raw_files = [os.path.join(windows_raw_path, x) for x in os.listdir(windows_raw_path)]
    filt_raws = []
    # filter by activation/enzyme rules
    for raw_file in raw_files:
        skip = False
        for must_have_str in contain_strs:
            if must_have_str not in raw_file:
                skip = True
        if not skip:
            filt_raws.append(raw_file)

    # create symlinks
    for raw_filepath in filt_raws:
        new_path = os.path.join(param_subfolder, os.path.basename(raw_filepath))
        # NOTE: REQUIRES ADMIN PRIVILEGES TO SUCCEED
        try:
            os.symlink(raw_filepath, new_path)
        except FileExistsError:
            # rerunning on prev directory - ignore
            continue


def gen_multilevel_shell(run_containers, main_dir, run_folders=None):
    """
    Generate a shell script to navigate the generated file structure and run all requested analyses
    :param run_containers: list of run containers to generate a shell script to run all of
    :type run_containers: list[RunContainer]
    :param main_dir: directory in which to save output
    :param run_folders: if doing a combined run on an outer dir, want to run Philosopher on the run subfolders rather than the main dir
    :return: void
    """
    output_shell_name = os.path.join(main_dir, 'fragger_shell_multi{}.sh'.format(RUN_IN_PROGRESS))
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

        if not SERIAL_PHILOSOPHER:
            # don't run philosopher in shell because this has to be run in the docker container, and philosopher has to be run outside it
            if not RAW_FORMAT == '.d':
                # run philosopher in parallel
                shellfile.write('#********************Philosopher Runs*******************\n')
                if run_folders is None:
                    if run_containers[0].enzyme is not '':
                        shellfile.write('cd ../../\n')      # up two directories if using enzymes, since data is inside enzyme dir
                    else:
                        shellfile.write('cd ../\n')
                    write_multi_phil(shellfile, run_containers, all_subfolders, shell_base)
                else:
                    # multiple run subfolders. CD to each in the list in turn
                    for run_folder in run_folders:
                        shellfile.write('cd {}/__FraggerResults\n'.format(PrepFraggerRuns.update_folder_linux(run_folder)))
                        write_multi_phil(shellfile, run_containers, all_subfolders, shell_base)

        else:
            # old serial philosopher code. Does NOT currently support multi-enzyme mode or outer directory fanciness
            for index, subfolder in enumerate(all_subfolders):
                shellfile.write('# Philosopher {} of {}*************************************\n'.format(index + 1, len(all_subfolders)))
                shellfile.write('cd {}\n'.format(PrepFraggerRuns.update_folder_linux(subfolder)))
                phil_lines = gen_philosopher_lines(shell_base, run_containers[index], serial=True)
                for line in phil_lines:
                    shellfile.write(line)
                shellfile.write('\n')


def write_multi_phil(shellfile, run_containers, all_subfolders, shell_base):
    """
    helper for writing out the multitrheaded philosopher shell code to call in multiple places. Assumes that:
    1) shellfile is already open for writing
    2) a 'cd' to the correct outer directory has already been performed
    :return: void
    :rtype:
    """
    shellfile.write('for folder in *; do\n')  # loop over everything in results directory
    shellfile.write('\tif [[ -d $folder ]]; then\n')  # if item is a directory, consider it
    shellfile.write('\t\tif [[ ! -e $folder/psm.tsv ]] && [[ -e $folder/phil.sh ]] ; then\n')  # don't run philosopher if psm.tsv already exists (prevent re-running old results)
    shellfile.write('\t\t\techo $folder\n')
    shellfile.write('\t\t\t$folder/phil.sh &> $folder/phil.log &\n')  # run philospher shell script
    shellfile.write('\t\tfi\n')
    shellfile.write('\tfi\n')
    shellfile.write('done\n')

    # if multienzyme, we also need to prep combined philosopher runs with manual parameters, since the individual philosopher shells don't work (b/c pipeline can't handle multienzyme)
    if run_containers[0].enzyme is not '':
        for subfolder in all_subfolders:
            current_container = None
            for run_container in run_containers:
                if run_container.subfolder == subfolder:
                    current_container = run_container
                    break
            phil_lines = gen_philosopher_lines(shell_base, current_container)
            # save phil.sh to each subfolder so the multithreaded philosopher shell code above has a phil.sh file to find
            output_shell_path = os.path.join(subfolder, 'phil.sh')
            with open(output_shell_path, 'w', newline='') as phil_shell:
                for line in phil_lines:
                    phil_shell.write(line)
                phil_shell.write('\n')


def gen_philosopher_lines(shell_template_lines, run_container: RunContainer, serial=False):
    """
    Generate philosopher instructions for a provided subfolder and shell template
    :param shell_template_lines: lines from standard template passed to run container
    :param run_container: run container
    :return: list of strings to append to file
    """
    if serial:
        output = []
    else:
        output = ['#!/bin/bash\nset -xe\n\n']
    if run_container.enzyme is not '':
        output.append('cd {}\n'.format(PrepFraggerRuns.update_folder_linux(run_container.subfolder)))
        # add check to avoid running on empty directories and filling up /tmp/
        output = check_empty_phil(output)

    for line in shell_template_lines:
        if line.startswith('toolDirPath') or line.startswith('philosopherPath') or line.startswith('$philosopherPath'):
            if line.startswith('$philosopherPath pipeline'):
                if run_container.enzyme is '':
                    output.append('$philosopherPath pipeline --config {} ./\n'.format(run_container.yml_file))
                else:
                    # CANNOT run pipeline for multi-enzyme data because it doesn't expect multiple interact.pep.xml files. Run manually
                    print('WARNING: protein prophet, filter, and report commands are hard-coded for multi-enzyme mode and will NOT be read from your yml')
                    output.append('fastaPath="{}"\n'.format(run_container.database_file))
                    output.append('$philosopherPath database --annotate $fastaPath --prefix $decoyPrefix\n')
                    output.append('$philosopherPath proteinprophet --maxppmdiff 2000000000 ./*.pep.xml\n')
                    output.append('$philosopherPath filter --sequential --razor --mapmods --pepxml . --protxml ./interact.prot.xml --models\n')
                    output.append('$philosopherPath report --decoys\n')
            else:
                output.append(line)
        if line.startswith('analysisName') or line.startswith('cp ') or line.startswith('decoy'):
            output.append(line)
    return output


def edit_yml(yml_file, database_path, disable_peptide_prophet=False):
    """
    Edit the yml file to include the fasta database path
    :param yml_file: path string
    :param database_path: path string
    :param disable_peptide_prophet: for multi-enzyme searches
    :return: void
    """
    lines = []
    with open(yml_file, 'r') as readfile:
        for line in list(readfile):
            if line.startswith('  protein_database'):
                line = '  protein_database: {}\n'.format(database_path)
            if line.startswith('  peptideprophet'):
                if disable_peptide_prophet:
                    line = '  peptideprophet: no\n'
            lines.append(line)
    with open(yml_file, 'w') as outfile:
        for line in lines:
            outfile.write(line)


def check_empty_phil(output):
    """
    Add check for empty files to philosopher shell script
    :param output: list of string lines to save
    :type output: list
    :return: updated output
    :rtype: list
    """
    output.append('set +x\nfound=false\n')
    output.append('for file in ./*.pepXML; do\n')
    output.append('\tif [ -e $file ]; then\n')
    output.append('\t\tfound=true\n\t\tbreak\n')
    output.append('\tfi\ndone\n')
    output.append('for file in ./*.pep.xml; do\n')
    output.append('\tif [ -e $file ]; then\n')
    output.append('\t\tfound=true\n\t\tbreak\n')
    output.append('\tfi\ndone\n')
    output.append('if [ $found == false ]; then\n')
    output.append('\techo "no .pepXML files found. NOT running Philosopher"\n\texit\nfi\nset -x\n')
    return output


def make_yml_peptideproph_only(yml_base, database_path, enzyme, output_subfolder):
    """
    For multi-enzyme search, make a yml file that only runs peptide prophet in the enyzme subdirectory
    :param yml_base: base yml file path
    :param database_path: path to DB **corrected for subfolder**
    :param enzyme: enzyme string
    :param output_subfolder: where to save the edited file
    :return: void
    """
    lines = []
    with open(yml_base, 'r') as readfile:
        for line in list(readfile):
            if line.startswith('  protein_database'):
                line = '  protein_database: {}\n'.format(database_path)

            # turn off the other pipeline items
            elif line.startswith('  proteinprophet'):
                line = '  proteinprophet: no\n'
            elif line.startswith('  filter'):
                line = '  filter: no\n'
            elif line.startswith('  report'):
                line = '  report: no\n'
            elif line.startswith('  enzyme'):
                line = '  enzyme: {}\n'.format(EditParams.ENZYME_DATA[enzyme][0])
            lines.append(line)

    # write output
    new_path = os.path.join(output_subfolder, os.path.basename('phil_config.yml'))
    with open(new_path, 'w') as outfile:
        for line in lines:
            outfile.write(line)
    return new_path


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
    if run_container.enzyme_subfolder is not '':
        subfolder = run_container.enzyme_subfolder
    else:
        subfolder = run_container.subfolder

    new_shell_name = os.path.join(subfolder, 'fragger_shell.sh')
    phil_shell_name = os.path.join(subfolder, 'phil.sh')
    shell_lines = PrepFraggerRuns.read_shell(run_container.shell_template)
    output = []
    phil_output = []

    # add a 'cd' to allow pasting together
    linux_folder = PrepFraggerRuns.update_folder_linux(subfolder)
    if write_output:
        output.append('#!/bin/bash\nset -xe\n\n')
        phil_output.append('#!/bin/bash\nset -x\n\n')
    output.append('# Change dir to local workspace\ncd {}\n'.format(linux_folder))
    phil_output.append('# Change dir to local workspace\ncd {}\n'.format(linux_folder))
    phil_output = check_empty_phil(phil_output)

    for line in shell_lines:
        if line.startswith('fasta'):
            if run_container.enzyme is not '':
                output.append('fastaPath="../{}"\n'.format(run_container.database_file))
                phil_output.append('fastaPath="../{}"\n'.format(run_container.database_file))
            else:
                output.append('fastaPath="{}"\n'.format(run_container.database_file))
                phil_output.append('fastaPath="{}"\n'.format(run_container.database_file))
        elif line.startswith('fraggerParams'):
            output.append('fraggerParamsPath="./{}"\n'.format(os.path.basename(run_container.param_file)))
            # output.append('fraggerParamsPath="{}"\n'.format(PrepFraggerRuns.update_folder_linux(run_container.param_file)))
        elif line.startswith('dataDirPath'):
            output.append('dataDirPath="{}"\n'.format(run_container.raw_path))
        elif line.startswith('msfraggerPath'):
            output.append('msfraggerPath=$toolDirPath/{}\n'.format(run_container.fragger_path))

        elif line.startswith('java'):
            # main Fragger command
            if run_container.split_dbs > 0:
                # using split DB, so prepend the python script before the fragger command
                fragger_cmd = '{} {} {} "java -Xmx{}G -jar" '.format(SPLIT_PYTHON_PATH, SPLIT_DB_SCRIPT, run_container.split_dbs, run_container.fragger_mem)
            else:
                # not using split DB
                fragger_cmd = 'java -Xmx{}G -jar '.format(run_container.fragger_mem)
            if run_container.activation_type is '':
                if run_container.enzyme is '':
                    fragger_cmd += '$msfraggerPath $fraggerParamsPath $dataDirPath/*{}\n'.format(run_container.raw_format)
                else:
                    fragger_cmd += '$msfraggerPath $fraggerParamsPath $dataDirPath/*_{}*{}\n'.format(run_container.enzyme, run_container.raw_format)
            else:
                if run_container.enzyme is '':
                    fragger_cmd += '$msfraggerPath $fraggerParamsPath $dataDirPath/*_{}{}\n'.format(run_container.activation_type, run_container.raw_format)
                else:
                    fragger_cmd += '$msfraggerPath $fraggerParamsPath $dataDirPath/*_{}*_{}*{}\n'.format(run_container.enzyme, run_container.activation_type, run_container.raw_format)
            output.append('start_time=$(date)\n')
            output.append(fragger_cmd)
            output.append('end_time=$(date)\n')

        elif line.startswith('$philosopherPath pipeline'):
            if run_philosopher:
                output.append('$philosopherPath pipeline --config {} ./\n'.format(run_container.yml_file))
                phil_output.append('$philosopherPath pipeline --config {} ./\n'.format(run_container.yml_file))
        elif line.startswith('$philosopherPath'):
            if run_philosopher:
                output.append(line)
                phil_output.append(line)
        elif line.startswith('analysisName') or line.startswith('cp '):
            if run_philosopher:
                output.append(line)
                phil_output.append(line)
        elif line.startswith('philosopherPath'):
            output.append(line)
            phil_output.append(line)
        elif line.startswith('toolDirPath'):
            output.append(line)
            phil_output.append(line)
        else:
            output.append(line)

    if run_container.enzyme is not '':
        # add peptide prophet run and copying out of subfolder
        output.append('$philosopherPath workspace --clean\n$philosopherPath workspace --init\n$philosopherPath pipeline --config phil_config.yml ./\nanalysisName=${PWD##*/}\ncp ./interact.pep.xml ../${analysisName}_interact.pep.xml\n\n')
        phil_output.append('$philosopherPath workspace --clean\n$philosopherPath workspace --init\n$philosopherPath pipeline --config phil_config.yml ./\nanalysisName=${PWD##*/}\ncp ./interact.pep.xml ../${analysisName}_interact.pep.xml\n\n')

    if write_output:
        with open(new_shell_name, 'w', newline='') as shellfile:
            for line in output:
                shellfile.write(line)
        with open(phil_shell_name, 'w', newline='') as philfile:
            for line in phil_output:
                philfile.write(line)
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


def batch_template_run(override_maindir):
    """
    Select and load template file(s) to be run
    :return: void
    """
    templates = filedialog.askopenfilenames(filetypes=[('Templates', '.csv')])
    # Get set(s) of runs from each template and generate a multilevel shell for each
    for template in templates:
        template_run_list, main_dir, run_folders = parse_template(template, override_maindir)
        # if len(template_run_list) > 1:
        if not override_maindir:
            gen_multilevel_shell(template_run_list, main_dir, run_folders)
        else:
            gen_multilevel_shell(template_run_list, main_dir)


def parse_template(template_file, override_maindir=True):
    """
    Read a template into a list of run containers to be run together (or several lists if multiple provided)
    :param template_file: path to template csv
    :param override_maindir: if true, use param path directory as main dir; otherwise, use provided main dir from template file
    :return: list of list of RunContainers
    """
    run_list = []
    run_folders = []
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
                        if not override_maindir:
                            raise ValueError('Main dir was not read on line {}, breaking'.format(line))
                if not override_maindir:
                    return_maindir = current_maindir    # this maindir is used ONLY for the final shell. All params/results stay in their subfolders

            elif line is not '\n':
                # add new analysis to the current
                param_path = splits[0]
                current_maindir = os.path.dirname(param_path)
                if override_maindir:
                    return_maindir = current_maindir
                if current_maindir not in run_folders:
                    run_folders.append(current_maindir)
                # param_path = os.path.join(current_maindir, splits[0])
                if not param_path.endswith('.params'):
                    param_path += '.params'
                if QUANT_COPY_ANNOTATION_FILE:
                    annotation_files = find_specific_files(current_maindir, ['annotation.txt'])
                    if len(annotation_files) == 1:
                        annotation_file = annotation_files[0]
                    else:
                        print('WARNING: annotation file requested but {} found, skipping'.format(len(annotation_files)))
                        annotation_file = ''
                else:
                    annotation_file = ''

                activation_types = splits[1].split(';')
                enzymes = splits[5].split(';')
                # raw path(s)
                raw_path_list = [PrepFraggerRuns.update_folder_linux(x) for x in splits[4].split(';')]
                raw_names_list = splits[6].split(';')
                try:
                    msfragger_jar = splits[7]
                    if msfragger_jar == '':
                        msfragger_jar = FRAGGER_JARNAME
                except IndexError:
                    msfragger_jar = FRAGGER_JARNAME
                run_container_list = prepare_runs_yml(params_files=[param_path], yml_files=[splits[2]],
                                                      raw_path_files=raw_path_list, shell_template=splits[3],
                                                      main_dir=current_maindir, activation_types=activation_types,
                                                      enzymes=enzymes, raw_names_list=raw_names_list, fragger_jar=msfragger_jar,
                                                      annotation_path=annotation_file)
                run_list.extend(run_container_list)
    return run_list, return_maindir, run_folders


def find_specific_files(starting_dir, endswith=None):
    """
    From starting dir, keep jumping up directories until finding file(s) ending with '_byonic.csv'. Returns those
    files
    :param starting_dir: dir to start (path)
    :type starting_dir: str
    :param endswith: list of file types to find
    :type endswith: list
    :return: list of filepaths
    """
    if endswith is None:
        endswith = ['byonic.csv']

    files = []
    for filetype in endswith:
        current_files = [os.path.join(starting_dir, x) for x in os.listdir(starting_dir) if x.lower().endswith(filetype.lower())]
        files.extend(current_files)
    jumps = 0
    while len(files) == 0:
        # jump up second directory if none found
        starting_dir = os.path.dirname(starting_dir)
        for filetype in endswith:
            current_files = [os.path.join(starting_dir, x) for x in os.listdir(starting_dir) if x.lower().endswith(filetype.lower())]
            files.extend(current_files)
        jumps += 1
        if jumps > 5:
            print('no files of type {} found!'.format(endswith))
            break
    return files


if __name__ == '__main__':
    root = tkinter.Tk()
    root.withdraw()

    # main(batch_mode=False)
    # main(batch_mode=True)
    batch_template_run(OVERRIDE_MAINDIR)
