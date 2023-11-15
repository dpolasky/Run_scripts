"""
Module for automatically running build tests on a FragPipe/MSFragger/Philosopher/IonQuant version combo.
Analogous to the old BuildTest_Fragger, but using FragPipe_Batch_Runner for headless FP running.

To Use:
- copy the FragPipe, MSFragger, Philosopher, and IonQuant to test into the "tools" folder
- set up the template with the analysis name, tool versions, and workflow template paths (NOTE: just need matching versions for tools, not full path)
- run the script
- run the generate bash script
- analyze results
"""

import os
import pathlib
import Fragpipe_Batch_Runner

TEST_TEMPLATE = r"Z:\dpolasky\projects\_BuildTests\_FragPipeTest_template.tsv"
TOOLS_FOLDER = r"Z:\dpolasky\projects\_BuildTests\tools"
OUTPUT_FOLDER = r"Z:\dpolasky\projects\_BuildTests\_results"


def parse_workflow_template(tools_folder, output_folder, outer_template_splits):
    """
    parse the workflow template, making a run for each line
    :param tools_folder: path to tools dir
    :type tools_folder: str
    :param output_folder: output base dir
    :type output_folder: pathlib.Path
    :param outer_template_splits: list of info from outer template (see below)
    :type outer_template_splits: list
    :return: list of FragPipe Runs
    :rtype: list
    """
    runs = []
    # resolve paths from version names
    fragpipe_path = [os.path.join(tools_folder, x) for x in os.listdir(tools_folder) if outer_template_splits[2].lower() in x.lower()][0]
    msfragger_path = [os.path.join(tools_folder, x) for x in os.listdir(tools_folder) if outer_template_splits[3].lower() in x.lower()][0]
    phil_path = [os.path.join(tools_folder, x) for x in os.listdir(tools_folder) if outer_template_splits[4].lower() in x.lower()][0]
    ion_quant_path = [os.path.join(tools_folder, x) for x in os.listdir(tools_folder) if outer_template_splits[5].lower() in x.lower()][0]

    with open(outer_template_splits[1], 'r') as readfile:
        for line in readfile:
            if line.startswith('#'):
                continue
            inner_splits = line.split('\t')

            # fragpipe_path = pathlib.Path(tools_folder) / outer_template_splits[2]
            # check workflow exists for this FragPipe version
            workflow_path = pathlib.Path(fragpipe_path) / 'workflows' / f'{inner_splits[0]}.workflow'
            if not os.path.exists(workflow_path):
                print(f'Warning: workflow {workflow_path} does not exist! skipping')

            fragpipe_run = Fragpipe_Batch_Runner.FragpipeRun(fragpipe=str(pathlib.Path(fragpipe_path) / 'bin' / 'fragpipe'),
                                                             workflow=workflow_path,
                                                             manifest=str(pathlib.Path(tools_folder).parent / 'manifests' / inner_splits[1]),
                                                             output=str(output_folder / inner_splits[0]),
                                                             ram=outer_template_splits[6],
                                                             threads=outer_template_splits[7],
                                                             msfragger=msfragger_path,
                                                             philosopher=phil_path,
                                                             ionquant=ion_quant_path,
                                                             python="",
                                                             skip_MSFragger=None,
                                                             database_path=str(pathlib.Path(tools_folder).parent / 'databases' / inner_splits[2]),
                                                             disable_list=None
                                                             )
            runs.append(fragpipe_run)
    return runs


def parse_template(template_file, tools_folder, output_folder):
    """
    Parse the template file and generate a dictionary of run name: [list of FragPipeRun objects]
    :param template_file: template with name, workflow template path, fragpipe, msfragger, phil, IQ paths, and ram/threads
    :type template_file: str
    :param tools_folder: path to tools dir
    :type tools_folder: str
    :param output_folder: output base dir
    :type output_folder: str
    :return: dict
    :rtype: dict
    """
    runs_dict = {}
    with open(template_file, 'r') as readfile:
        for line in readfile:
            if line.startswith('#'):
                continue
            splits = line.rstrip('\n').split('\t')
            name = splits[0]
            output_dir = pathlib.Path(output_folder) / name
            runs_list = parse_workflow_template(tools_folder, output_dir, splits)
            runs_dict[name] = runs_list
    return runs_dict


def main():
    """
    generate a bash script for running the tests
    :return: void
    :rtype:
    """
    runs_dict = parse_template(TEST_TEMPLATE, TOOLS_FOLDER, OUTPUT_FOLDER)

    is_first_run = True
    all_bash = []
    for run_name, run_list in runs_dict.items():
        output_dir = pathlib.Path(OUTPUT_FOLDER) / run_name
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        linux_commands = Fragpipe_Batch_Runner.make_commands_linux(run_list, output_dir, write_output=False, is_first_run=is_first_run)
        is_first_run = False
        all_bash.extend(linux_commands)

    bash_script_path = pathlib.Path(TEST_TEMPLATE).parent / 'fragpipe_batch.sh'
    with open(bash_script_path, 'w', newline='') as outfile:
        for line in all_bash:
            outfile.write(line)


if __name__ == '__main__':
    main()
