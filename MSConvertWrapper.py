"""
Command line runner for MSConvert
"""

import tkinter
from tkinter import filedialog
import os
import subprocess
import multiprocessing

tool_path = r"C:\Program Files\ProteoWizard\ProteoWizard 3.0.19194.9338c77b2\msconvert.exe"
out_dir = ''
DEISOTOPE = True
RUN_BOTH = False
THREADS = 12

MAX_CHARGE = 6
ACTIVATION_LIST = ['ETD', 'HCD']   # 'ETD', 'HCD', etc. IF NOT USING, SET TO ['']
# ACTIVATION_LIST = ['']


def format_commands(filename, deisotope, activation_method=''):
    """
    Generate a formatted string for running MSConvert
    :param filename:
    :param deisotope: boolean
    :param activation_method: list of activation strings, run each in separate file
    :return: string
    """
    # create output directory
    if deisotope:
        output_dir = os.path.join(os.path.dirname(filename), '{}_deiso'.format(activation_method))
    else:
        output_dir = os.path.join(os.path.dirname(filename), '{}'.format(activation_method))
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    cmd_str = '{} {} --mzML -z --64 -o {}'.format(tool_path, filename, output_dir)
    cmd_str += ' --filter "peakPicking true 1-"'
    # remove 0 samples if converting from profile data (helps reduce file size a lot)
    cmd_str += ' --filter "zeroSamples removeExtra 1-"'

    # activation filter
    if activation_method is not None:
        cmd_str += ' --filter "activation {}"'.format(activation_method)

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

    commands = []
    for file in files:
        for activation in ACTIVATION_LIST:
            if RUN_BOTH:
                cmd = format_commands(file, deisotope=True, activation_method=activation)
                # subprocess.run(cmd)
                commands.append(cmd)
                cmd = format_commands(file, deisotope=False, activation_method=activation)
                # subprocess.run(cmd)
                commands.append(cmd)
            else:
                cmd = format_commands(file, deisotope=DEISOTOPE, activation_method=activation)
                # subprocess.run(cmd)
                commands.append(cmd)

    pool = multiprocessing.Pool(processes=THREADS)
    for cmd in commands:
        pool_result = pool.apply_async(run_cmd, args=[cmd])
    pool.close()
    pool.join()

