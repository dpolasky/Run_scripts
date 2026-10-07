"""
Module for automatically running build tests on a FragPipe/MSFragger/Philosopher/IonQuant version combo.
Analogous to the old BuildTest_Fragger, but using FragPipe_Batch_Runner for headless FP running.

To Use:
- copy the FragPipe, MSFragger, Philosopher, and IonQuant to test into the "tools" folder
- set up the template with the analysis name, tool versions, and workflow template paths (NOTE: just need matching versions for tools, not full path)
- run the script
- run the generate bash script
- analyze results with PSM_scripts/RunTools/FragPipe_Test_Results_Better.py (summary counts, per-tool timing, and
  regression comparisons of each workflow against its previous run)
"""

import os
import pathlib
import sys
import Fragpipe_Batch_Runner
FRAGPIPE_FOLDER = r"Z:\dpolasky\projects\_BuildTests\tools"
TOOLS_FOLDER = r"Z:\dpolasky\tools"
NEW_FRAGPIPE = True     # >21.2-build40 specifying tools folder, not individual paths

TEST_TEMPLATE = r"Z:\dpolasky\projects\_BuildTests\_FragPipeTest_template.tsv"
# TEST_TEMPLATE = r"Z:\dpolasky\projects\_BuildTests\_FragPipeTest_template_speedTest.tsv"
# TEST_TEMPLATE = r"Z:\dpolasky\projects\_BuildTests\_FragPipeTest_single.tsv"
OUTPUT_FOLDER = r"Z:\dpolasky\projects\_BuildTests\_results"
# OUTPUT_FOLDER = r"Z:\dpolasky\projects\_BuildTests\_other-testing"
# ADDITIONAL_WORKFLOWS_TEMPLATE = r"Z:\dpolasky\projects\_BuildTests\additional_test_workflows\template_no-raw.tsv"
ADDITIONAL_WORKFLOWS_TEMPLATE = None
# ADDITIONAL_WORKFLOWS_TEMPLATE = r"Z:\dpolasky\projects\_BuildTests\additional_test_workflows\template.tsv"

# parameters set in every test workflow. Keeping temp files retains the MSFragger pepXML/pin outputs (deleted by
# default in e.g. DIA workflows) so the MSFragger regression comparison can run for every workflow.
WORKFLOW_PARAM_OVERRIDES = {'tab-run.delete_temp_files': 'false'}
# WORKFLOW_PARAM_OVERRIDES = {}

# DISABLE_TOOLS = True
DISABLE_TOOLS = False
CLEAR_PREV_TEMP_FILES = False
# CLEAR_PREV_TEMP_FILES = True    # clear .pepindex files between runs (using folders below)
RAW_FOLDER = r"Z:\dpolasky\projects\_BuildTests\raw"
DB_FOLDER = r"Z:\dpolasky\projects\_BuildTests\databases"

# TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER]
# TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER, DisableTools.PEPTIDEPROPHET, DisableTools.PERCOLATOR, DisableTools.PSMVALIDATION]
# filter/report onwards (PTM-S, OPair, quant)
TOOLS_TO_DISABLE = [Fragpipe_Batch_Runner.DisableTools.MSFRAGGER, Fragpipe_Batch_Runner.DisableTools.PEPTIDEPROPHET, Fragpipe_Batch_Runner.DisableTools.PERCOLATOR, Fragpipe_Batch_Runner.DisableTools.PROTEINPROPHET, Fragpipe_Batch_Runner.DisableTools.PSMVALIDATION]
# TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER, DisableTools.PEPTIDEPROPHET, DisableTools.PERCOLATOR, DisableTools.PROTEINPROPHET, DisableTools.PSMVALIDATION, DisableTools.FILTERandREPORT]     # PTM-S or quant only
# TOOLS_TO_DISABLE = [DisableTools.PTMPROPHET]
# TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER, DisableTools.PEPTIDEPROPHET, DisableTools.PERCOLATOR, DisableTools.PSMVALIDATION, DisableTools.PTMPROPHET]
# TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER, DisableTools.PTMPROPHET]
# TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER, DisableTools.PEPTIDEPROPHET, DisableTools.PSMVALIDATION, DisableTools.PERCOLATOR]
# TOOLS_TO_DISABLE = [DisableTools.FREEQUANT, DisableTools.LFQ]
 # OPair, quant only
# TOOLS_TO_DISABLE = [DisableTools.MSFRAGGER, DisableTools.PEPTIDEPROPHET, DisableTools.PERCOLATOR, DisableTools.PROTEINPROPHET, DisableTools.PSMVALIDATION, DisableTools.FILTERandREPORT, DisableTools.PTMSHEPHERD]

if not DISABLE_TOOLS:
    TOOLS_TO_DISABLE = None


def parse_workflow_template(tools_folder, output_folder, outer_template_splits, disable_list=None):
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
    # resolve paths from version names (blank version = not specified, e.g. FragPipe uses the default tools folder)
    fragpipe_version = outer_template_splits[2].strip()
    if not fragpipe_version:
        print('Error: no FragPipe version specified for test {}!'.format(outer_template_splits[0]))
        sys.exit(1)
    fragpipe_path = resolve_tool_path(FRAGPIPE_FOLDER, 'fragpipe', fragpipe_version)
    msfragger_path = resolve_tool_path(tools_folder, 'MSFragger', outer_template_splits[3].strip(), extension='.jar')
    phil_path = resolve_tool_path(tools_folder, 'philosopher', outer_template_splits[4].strip())
    ion_quant_path = resolve_tool_path(tools_folder, 'IonQuant', outer_template_splits[5].strip(), extension='.jar')

    # add additional test workflows (not distributed with FragPipe) to each analysis
    if ADDITIONAL_WORKFLOWS_TEMPLATE is not None:
        for inner_splits in read_workflow_template(ADDITIONAL_WORKFLOWS_TEMPLATE):
            workflow_path = pathlib.Path(ADDITIONAL_WORKFLOWS_TEMPLATE).parent / f'{inner_splits[0]}.workflow'
            if not os.path.exists(workflow_path):
                print(f'Warning: workflow {workflow_path} does not exist! skipping')
                continue
            runs.append(make_single_run(fragpipe_path, msfragger_path, phil_path, ion_quant_path, tools_folder, output_folder, outer_template_splits, workflow_path, inner_splits, disable_list))

    # make main tests from built-in FragPipe workflows
    for inner_splits in read_workflow_template(outer_template_splits[1]):
        # check workflow exists for this FragPipe version
        workflow_path = pathlib.Path(fragpipe_path) / 'workflows' / f'{inner_splits[0]}.workflow'
        if not os.path.exists(workflow_path):
            print(f'Warning: workflow {workflow_path} does not exist! skipping')
            continue
        runs.append(make_single_run(fragpipe_path, msfragger_path, phil_path, ion_quant_path, tools_folder, output_folder, outer_template_splits, workflow_path, inner_splits, disable_list))
    return runs


def read_workflow_template(template_path):
    """
    Read a workflows template (workflow name, manifest, database, uniques config), skipping comments and blank lines
    :return: list of split lines
    :rtype: list[list[str]]
    """
    entries = []
    with open(template_path, 'r') as readfile:
        for line in readfile:
            if line.startswith('#') or not line.strip():
                continue
            entries.append([x.strip() for x in line.split('\t')])
    return entries


def resolve_tool_path(folder, prefix, version, extension=''):
    """
    Find the tool named '<prefix>-<version><extension>' in folder (case-insensitive), e.g. MSFragger-4.5-rc14.jar.
    Falls back to that file inside a same-named subfolder (e.g. MSFragger-4.2/MSFragger-4.2.jar), then to the only
    name that starts with the prefix and contains the version. Exits if the version can't be resolved uniquely.
    :param folder: folder containing tools
    :param prefix: tool name prefix (e.g. 'MSFragger')
    :param version: version string from the test template. Blank = not specified
    :param extension: file extension of the tool (e.g. '.jar'), or '' for folders/executables
    :return: full path, or '' if no version specified
    :rtype: str
    """
    if not version:
        return ''
    target = '{}-{}{}'.format(prefix, version, extension).lower()
    names = os.listdir(folder)
    for name in names:
        if name.lower() == target:
            return os.path.join(folder, name)
    if extension:
        for name in names:
            inner_path = os.path.join(folder, name, name + extension)
            if name.lower() == target[:-len(extension)] and os.path.exists(inner_path):
                return inner_path
    partial_matches = [x for x in names if x.lower().startswith(prefix.lower()) and version.lower() in x.lower()]
    if len(partial_matches) == 1:
        return os.path.join(folder, partial_matches[0])
    print('Error: could not find a unique {} version {} in {}. Candidates: {}'.format(prefix, version, folder, partial_matches))
    sys.exit(1)


def make_single_run(fragpipe_path, msfragger_path, phil_path, ion_quant_path, tools_folder, output_folder, outer_template_splits, workflow_path, inner_splits, disable_list):
    """
    Helper to make a single fragpipe_run from parsed template info
    """
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
                                                     disable_list=disable_list
                                                     )
    Fragpipe_Batch_Runner.edit_workflow_params(fragpipe_run.workflow_path, WORKFLOW_PARAM_OVERRIDES)
    return fragpipe_run


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
            runs_list = parse_workflow_template(tools_folder, output_dir, splits, TOOLS_TO_DISABLE)
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
        print('preparing {}'.format(run_name))
        output_dir = pathlib.Path(OUTPUT_FOLDER) / run_name
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        linux_commands = Fragpipe_Batch_Runner.make_commands_linux(run_list, output_dir, NEW_FRAGPIPE, write_output=False, is_first_run=is_first_run)
        if CLEAR_PREV_TEMP_FILES:
            linux_commands.append('rm {}/*.pepindex\n'.format(Fragpipe_Batch_Runner.update_folder_linux(DB_FOLDER)))
            # linux_commands.append('rm {}/*.fragtmp\n'.format(Fragpipe_Batch_Runner.update_folder_linux(RAW_FOLDER)))
        is_first_run = False
        all_bash.extend(linux_commands)

    bash_script_path = pathlib.Path(TEST_TEMPLATE).parent / 'fragpipe_batch.sh'
    with open(bash_script_path, 'w', newline='') as outfile:
        for line in all_bash:
            outfile.write(line)


if __name__ == '__main__':
    main()
