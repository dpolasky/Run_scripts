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
THREADS = 6

MAX_CHARGE = 5
ACTIVATION_LIST = ['HCD', 'AIETD']   # 'ETD', 'HCD', etc. IF NOT USING, SET TO ['']
# ACTIVATION_LIST = ['AIETD']

# ACTIVATION_LIST = ['']


def format_commands(filename, deisotope, output_dir, activation_method=''):
    """
    Generate a formatted string for running MSConvert
    :param filename:
    :param deisotope: boolean
    :param output_dir
    :param activation_method: list of activation strings, run each in separate file
    :return: string
    """
    output_path = os.path.join(os.path.dirname(file), output_dir)
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
            actual_activation = activation
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


if __name__ == '__main__':
    root = tkinter.Tk()
    root.withdraw()

    files = filedialog.askopenfilenames(filetypes=[('Raw', '.raw')])
    files = [os.path.join(os.path.dirname(x), x) for x in files]
    maindir = os.path.dirname(files[0])
    pool = multiprocessing.Pool(processes=THREADS)

    for activation in ACTIVATION_LIST:
        # create output directory
        if DEISOTOPE:
            outputdir = os.path.join(maindir, '{}_deiso'.format(activation))
        else:
            outputdir = os.path.join(maindir, activation)
        if not os.path.exists(outputdir):
            os.makedirs(outputdir)

        commands = []
        for file in files:
            if RUN_BOTH:
                cmd = format_commands(file, deisotope=True, activation_method=activation, output_dir=outputdir)
                commands.append(cmd)
                cmd = format_commands(file, deisotope=False, activation_method=activation, output_dir=outputdir)
                commands.append(cmd)
            else:
                cmd = format_commands(file, deisotope=DEISOTOPE, activation_method=activation, output_dir=outputdir)
                commands.append(cmd)

        results = []
        for cmd in commands:
            pool_result = pool.apply_async(run_cmd, args=[cmd])
            results.append(pool_result)

        for result in results:
            test = result.get()

        # rename files by activation type
        outputfiles = [os.path.join(outputdir, x) for x in os.listdir(outputdir)]
        for outputfile in outputfiles:
            old_filename = os.path.splitext(os.path.basename(outputfile))[0]
            new_filename = '{}_{}{}'.format(old_filename, activation, os.path.splitext(outputfile)[1])
            new_path = os.path.join(maindir, new_filename)
            os.rename(outputfile, new_path)

    # pool.join()
    pool.close()
