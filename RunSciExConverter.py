"""
Script for running ABSciEx converter (wiff -> mzML)
"""

import tkinter
from tkinter import filedialog
import os
import subprocess
import multiprocessing

tool_path = r"C:\Program Files (x86)\AB SCIEX\MS Data Converter\AB_SCIEX_MS_Converter.exe"
# tool_path = 'AB_SCIEX_MS_Converter.exe'


def run_converter(wiff_files):
    """
    run converter on the provided files
    :param wiff_files:
    :return:
    """
    for file in wiff_files:
        output_path = os.path.join(os.path.dirname(file), os.path.splitext(os.path.basename(file))[0] + '.mzML')
        command = '{} WIFF "{}" MZML "{}" -i -doubleprecision -zlib'.format(tool_path, file, output_path)
        print(command)
        subprocess.run(command)


if __name__ == '__main__':
    root = tkinter.Tk()
    root.withdraw()

    files = filedialog.askopenfilenames(filetypes=[('wiff', '.wiff')])
    files = [os.path.join(os.path.dirname(x), x) for x in files]

    run_converter(files)
