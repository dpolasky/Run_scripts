"""
Command line runner for MSConvert
"""

import tkinter
from tkinter import filedialog
import os
import subprocess
import multiprocessing
import time
import RemoveScans_mzML

CHECK_ONLY = False  # do not actually run MSConvert if True, only validate that files are all converted successfully

tool_path = r"C:\Users\dpolasky\AppData\Local\Apps\ProteoWizard 3.0.19296.ebe17a86f 64-bit\msconvert.exe"
out_dir = ''
DEISOTOPE = False
DEISO_TIME_TEST = False
RUN_BOTH = False
THREADS = 8
MAX_CHARGE = 6
# ACTIVATION_LIST = ['HCD', 'AIETD']   # 'ETD', 'HCD', etc. IF NOT USING, SET TO ['']
ACTIVATION_LIST = None
# ACTIVATION_LIST = ['AIETD']
# ACTIVATION_LIST = ['HCD']
# ACTIVATION_LIST = ['HCD', 'EThcD']  # FOLLOW-UP REQUIRED! MSConvert cannot distinguish. Use RemoveScans_mzML.py!
# ACTIVATION_LIST = ['EThcD']    # FOLLOW-UP REQUIRED! MSConvert cannot distinguish. Use RemoveScans_mzML.py!
# ACTIVATION_LIST = ['HCD', 'ETD']
# ACTIVATION_LIST = ['ETD']


def format_commands(filename, deisotope, output_dir, activation_method=None, deiso_time_test=False, deiso_poisson=True):
    """
    Generate a formatted string for running MSConvert
    :param filename:
    :param deisotope: boolean
    :param output_dir
    :param activation_method: list of activation strings, run each in separate file
    :return: string
    """
    output_path = os.path.join(os.path.dirname(filename), output_dir)
    # output_file = os.path.join(output_path, os.path.basename(filename))
    cmd_str = '{} {} --64 -o {}'.format(tool_path, filename, output_path)
    if not deiso_time_test:
        cmd_str += ' -z --filter "peakPicking true 1-"'
        # remove 0 samples if converting from profile data (helps reduce file size a lot)
        cmd_str += ' --filter "zeroSamples removeExtra 1-"'
    else:
        cmd_str += ' -v --mgf'
        # cmd_str += ' --filter "peakPicking true 1-"'

        # cmd_str += ' --filter msLevel 2-'

    # activation filter
    if activation_method is not None:
        if activation_method in ['AIETD', 'EThcD']:
            actual_activation = 'ETD'
        else:
            actual_activation = activation_method
        cmd_str += ' --filter "activation {}"'.format(actual_activation)

    if deisotope:
        if deiso_poisson:
            cmd_str += ' --filter "MS2Deisotope Poisson minCharge=1 maxCharge={}"'.format(MAX_CHARGE)
        else:
            cmd_str += ' --filter "MS2Deisotope hi_res"'

    print(cmd_str)
    return cmd_str


def run_cmd(command_str):
    """
    Run subprocess
    :param command_str: string command
    :return: void
    """
    start = time.time()
    subprocess.run(command_str)
    end = time.time() - start
    print('time {:.1f} s for {}'.format(end, command_str))
    return command_str


def check_results(pool, results_list, commands_list):
    """
    Check if results have finished with set timeout period and rerun them if not
    :param pool:
    :param results_list:
    :param commands_list:
    :return:
    """
    results2 = []
    for index, result in enumerate(results_list):
        try:
            test = result.get(timeout=20)
        except multiprocessing.context.TimeoutError:
            # this result failed and needs to be rerun
            test_cmd = commands_list[index]
            result2 = pool.apply_async(run_cmd, args=[test_cmd])
            results2.append(result2)

    return results2


def run_msconvert(raw_files, activation_types=None, deisotope=False):
    """
    Main method to run msConvert multithreaded with optional splitting by activation types
    :param raw_files: list of raw file paths to convert
    :param activation_types: list of strings or None
    :param deisotope: whether to desiotope with MSConvert
    :return: void
    """
    maindir = os.path.dirname(files[0])

    if activation_types is None:
        # just run without splitting/filtering
        activation_types = [None]

    for activation in activation_types:
        # create output directory
        if DEISOTOPE:
            if activation is None:
                outputdir = os.path.join(maindir, 'deiso')
            else:
                outputdir = os.path.join(maindir, '{}_deiso'.format(activation))
        else:
            if activation is None:
                outputdir = maindir
            else:
                outputdir = os.path.join(maindir, activation)
        if not os.path.exists(outputdir):
            os.makedirs(outputdir)

        commands = []
        for file in raw_files:
            cmd = format_commands(file, deisotope=deisotope, activation_method=activation, output_dir=outputdir)
            commands.append(cmd)

        if THREADS > 1:
            pool = multiprocessing.Pool(processes=THREADS)

            results = []
            for cmd in commands:
                pool_result = pool.apply_async(run_cmd, args=[cmd])
                results.append(pool_result)

            results_check = check_results(pool, results, commands)
            results_check2 = check_results(pool, results_check, commands)

            # pool.join()
            pool.terminate()
            pool.close()
        else:
            # single thread
            for cmd in commands:
                run_cmd(cmd)

        # kill any remaining MSConvert processes because we're definitely done and having them hang around can cause problems...
        # kill_msconvert_procs()

        # rename files by activation type
        if activation is not None:
            outputfiles = [os.path.join(outputdir, x) for x in os.listdir(outputdir)]
            rename_files_activation(activation, outputfiles)

    output_files = [os.path.join(maindir, x) for x in os.listdir(maindir) if x.endswith('.mzML')]
    print('checking {} file outputs...'.format(len(output_files)))
    bad_files = check_converted_files(output_files)
    # for file in bad_files:
    #     print('Bad: {}'.format(file))
    if len(bad_files) == 0:
        print('All files extracted successfully')


def rename_files_activation(activation, outputfiles, do_ethcd_fix=False):
    """
    Rename activation files to file_activation.mzML if needed. Also run remove_scans_mzML if needed for HCD/EThcD combo
    :param activation: activation string
    :type activation: str
    :param outputfiles: list of files to consider
    :type outputfiles: list
    :param do_ethcd_fix: if True, remove scans containing ETD from EThcD (to give HCD only)
    :return: void
    :rtype:
    """
    maindir = os.path.dirname(outputfiles[0])
    for outputfile in outputfiles:
        old_filename = os.path.splitext(os.path.basename(outputfile))[0]
        new_filename = '{}_{}{}'.format(old_filename, activation, os.path.splitext(outputfile)[1])
        new_path = os.path.join(maindir, new_filename)
        try:
            os.rename(outputfile, new_path)
        except PermissionError:
            print('Permission error for file {}'.format(outputfile))
            kill_msconvert_procs()
            os.rename(outputfile, new_path)     # todo: can it still crash here?

        # if EThcD, fix files here too
        if do_ethcd_fix:
            RemoveScans_mzML.filter_scans(new_path, [RemoveScans_mzML.ActivationType.HCD], maindir)  # keep HCD only (e.g. do on 'HCD' output of EThcD conversion to remove EThcD scans from the HCD file)


def check_converted_files(file_list):
    """
    Figure out which (if any) files did not finish. (Sometimes MSConvert process hang and continue
    even after their calling workers have been terminated)
    :param file_list: list of file paths
    :return: list of bad files
    """
    bad_files = []
    for index, file in enumerate(file_list):
        print('checking file {} of {}'.format(index + 1, len(file_list)))
        with open(file, 'r') as readfile:
            last_line = list(readfile)[-1]
            if '</indexedmzML' not in last_line:
                bad_files.append(file)
                print('Bad: {}'.format(file))
    return bad_files


def kill_msconvert_procs():
    """
    stop all msconvert.exe processes
    :return: void
    """
    handle = subprocess.Popen("msconvert.exe", shell=False)
    subprocess.Popen("taskkill /F /T /PID %i" % handle.pid, shell=True)


def singlethr_guarantee_slow_method(raw_files, activation_types, deisotope):
    """
    Single threaded version with extra redundancy to try to guarantee that files finish converting successfully
    even when errors are hit. Note: errors occur when converting Thermo files, if there is an empty scan, and the
    remove 0's filter is specified, but only sometimes (?). Checks and re-runs failed processes after killing the
    previous MSConvert process if it's still running.
    :param raw_files: list of raw files to convert
    :type raw_files: list
    :param activation_types: list of strings or None
    :type activation_types: list
    :param deisotope: whether to desiotope with MSConvert
    :type deisotope: bool
    :return: void
    :rtype:
    """
    outputdir = os.path.dirname(raw_files[0])
    if activation_types is None:
        activation_types = [None]

    for activation in activation_types:
        output_files = []
        for file in raw_files:
            cmd = format_commands(file, deisotope=deisotope, activation_method=activation, output_dir=outputdir)
            output_path = inner_loop(cmd, file)
            output_files.append(output_path)
        if activation is not None:
            if 'EThcD' in activation_types:
                if activation == 'HCD':
                    fix_hcd = True
                else:
                    fix_hcd = False
            else:
                fix_hcd = False
            rename_files_activation(activation, output_files, fix_hcd)

    # check all files again at the end
    output_files = [os.path.join(outputdir, x) for x in os.listdir(outputdir) if x.endswith('.mzML')]
    print('checking {} file outputs...'.format(len(output_files)))
    bad_files = check_converted_files(output_files)
    if len(bad_files) == 0:
        print('All files extracted successfully')


def inner_loop(cmd, filepath):
    """
    Inner loop of actual file conversion to enable running until MSConvert gets it right
    :param cmd: command string to pass to MSConvert
    :type cmd: str
    :param filepath: file being converted (full path)
    :type filepath: str
    :return:
    :rtype:
    """
    run_cmd(cmd)
    # check output
    output_path = os.path.splitext(filepath)[0] + '.mzML'
    bad_files = check_converted_files([output_path])
    if len(bad_files) > 0:
        print('retrying conversion in file {}'.format(os.path.basename(filepath)))
        # conversion failed - kill MSConvert and retry
        kill_msconvert_procs()
        inner_loop(cmd, filepath)
    return output_path


if __name__ == '__main__':
    root = tkinter.Tk()
    root.withdraw()

    files = filedialog.askopenfilenames(filetypes=[('Raw', '.raw'), ('mzML', '.mzml')])
    files = [os.path.join(os.path.dirname(x), x) for x in files]

    if CHECK_ONLY:
        check_converted_files(files)
    else:
        # run_msconvert(files, ACTIVATION_LIST, DEISOTOPE)
        singlethr_guarantee_slow_method(files, ACTIVATION_LIST, DEISOTOPE)
    # main_dir = os.path.dirname(files[0])
    # output_files = [os.path.join(main_dir, x) for x in os.listdir(main_dir) if x.endswith('.mzML')]
    # bad_files = check_converted_files(output_files)
    # for file in bad_files:
    #     print('Bad: {}'.format(file))

