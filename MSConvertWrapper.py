"""
Command line runner for MSConvert
"""

import tkinter
from tkinter import filedialog
import os
import subprocess
import multiprocessing

tool_path = r"C:\Users\dpolasky\AppData\Local\Apps\ProteoWizard 3.0.19296.ebe17a86f 64-bit\msconvert.exe"
out_dir = ''
DEISOTOPE = False
RUN_BOTH = False
THREADS = 11
MAX_CHARGE = 6
# ACTIVATION_LIST = ['HCD', 'AIETD']   # 'ETD', 'HCD', etc. IF NOT USING, SET TO ['']
ACTIVATION_LIST = None
# ACTIVATION_LIST = ['AIETD']
# ACTIVATION_LIST = ['HCD']
# ACTIVATION_LIST = ['HCD', 'EThcD']
# ACTIVATION_LIST = ['EThcD']
# ACTIVATION_LIST = ['HCD', 'ETD']
# ACTIVATION_LIST = ['ETD']


def format_commands(filename, deisotope, output_dir, activation_method=''):
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
    cmd_str = '{} {} --mzML -z --64 -o {}'.format(tool_path, filename, output_path)
    cmd_str += ' --filter "peakPicking true 1-"'
    # remove 0 samples if converting from profile data (helps reduce file size a lot)
    cmd_str += ' --filter "zeroSamples removeExtra 1-"'

    # activation filter
    if activation_method is not None:
        if activation_method in ['AIETD', 'EThcD']:
            actual_activation = 'ETD'
        else:
            actual_activation = activation_method
        cmd_str += ' --filter "activation {}"'.format(actual_activation)

    if deisotope:
        cmd_str += ' --filter "MS2Deisotope Poisson minCharge=1 maxCharge={}"'.format(MAX_CHARGE)
        # cmd_str += ' --filter "MS2Deisotope hi_res"'

    print(cmd_str)
    return cmd_str


def run_cmd(command_str):
    """
    Run subprocess
    :param command_str: string command
    :return: void
    """
    subprocess.run(command_str)
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
        pool = multiprocessing.Pool(processes=THREADS)

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

        results = []
        for cmd in commands:
            pool_result = pool.apply_async(run_cmd, args=[cmd])
            results.append(pool_result)

        results_check = check_results(pool, results, commands)
        results_check2 = check_results(pool, results_check, commands)

        # pool.join()
        pool.terminate()
        pool.close()

        # kill any remaining MSConvert processes because we're definitely done and having them hang around can cause problems...
        # kill_msconvert_procs()

        # rename files by activation type
        if activation is not None:
            outputfiles = [os.path.join(outputdir, x) for x in os.listdir(outputdir)]
            for outputfile in outputfiles:
                old_filename = os.path.splitext(os.path.basename(outputfile))[0]
                new_filename = '{}_{}{}'.format(old_filename, activation, os.path.splitext(outputfile)[1])
                new_path = os.path.join(maindir, new_filename)
                try:
                    os.rename(outputfile, new_path)
                except PermissionError:
                    print('Permission error for file {}'.format(outputfile))
                    continue
    output_files = [os.path.join(maindir, x) for x in os.listdir(maindir) if x.endswith('.mzML')]
    print('checking {} file outputs...'.format(len(output_files)))
    bad_files = check_converted_files(output_files)
    for file in bad_files:
        print('Bad: {}'.format(file))
    if len(bad_files) == 0:
        print('All files extracted successfully')


def check_converted_files(file_list):
    """
    Figure out which (if any) files did not finish. (Sometimes MSConvert process hang and continue
    even after their calling workers have been terminated)
    :param file_list: list of file paths
    :return: list of bad files
    """
    bad_files = []
    for file in file_list:
        with open(file, 'r') as readfile:
            last_line = list(readfile)[-1]
            if '</indexedmzML' not in last_line:
                bad_files.append(file)
    return bad_files


def kill_msconvert_procs():
    """
    stop all msconvert.exe processes
    :return: void
    """
    handle = subprocess.Popen("msconvert.exe", shell=False)
    subprocess.Popen("taskkill /F /T /PID %i" % handle.pid, shell=True)


if __name__ == '__main__':
    root = tkinter.Tk()
    root.withdraw()

    files = filedialog.askopenfilenames(filetypes=[('Raw', '.raw')])
    files = [os.path.join(os.path.dirname(x), x) for x in files]

    run_msconvert(files, ACTIVATION_LIST, DEISOTOPE)

    # main_dir = os.path.dirname(files[0])
    # output_files = [os.path.join(main_dir, x) for x in os.listdir(main_dir) if x.endswith('.mzML')]
    # bad_files = check_converted_files(output_files)
    # for file in bad_files:
    #     print('Bad: {}'.format(file))

