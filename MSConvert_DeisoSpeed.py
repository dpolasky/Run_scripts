"""
Script for testing deisotoping speed for comparison in manuscript. Uses MSConvertWrapper for most of the actual work
"""

import MSConvertWrapper
import tkinter
from tkinter import filedialog
import os
import subprocess
import time


def timing_test(file_list):
    """
    Run deisotoping on provided file(s) and record time taken to do so in an output file
    :param file_list: list of files to deisotope (full paths)
    :type file_list: list
    :return: void
    :rtype:
    """
    times = {}
    output_dir = os.path.join(os.path.dirname(file_list[0]), 'Output')
    for file in file_list:
        command_str = MSConvertWrapper.format_commands(file, deisotope=True, output_dir=output_dir,
                                                       deiso_time_test=True,
                                                       deiso_poisson=True)
        runtime = run_cmd(command_str)
        times[os.path.basename(file)] = runtime

    # save output
    for filename, runtime in times.items():
        print('{:.1f}, {}'.format(runtime, filename))


def run_cmd(command_str):
    """
    Run subprocess, return time for the full process
    :param command_str: string command
    :return: time taken
    """
    start = time.time()
    subprocess.run(command_str)
    end = time.time() - start
    # print('time {:.1f} s for {}'.format(end, command_str))
    return end


if __name__ == '__main__':
    root = tkinter.Tk()
    root.withdraw()

    files = filedialog.askopenfilenames(filetypes=[('mzML', '.mzml'), ('Raw', '.raw')])
    files = [os.path.join(os.path.dirname(x), x) for x in files]

    timing_test(files)
