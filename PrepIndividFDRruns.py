"""
convenience wrapper for running individual runs since I seem to be doing this a lot
"""

# import tkinter
# from tkinter import filedialog
import os
import shutil


def prep_individ_run(results_folder, shell_file):
    """
    Generate a subfolder within the main results folder and run the shell script within that subfolder
    :param results_folder: folder
    :param shell_file: shell
    :return: void
    """
    subfolder = os.path.join(results_folder, '_individFDR')
    os.makedirs(subfolder)
    shutil.copyfile(shell_file, os.path.join(subfolder, os.path.basename(shell_file)))
    return subfolder


# if __name__ == '__main__':
#     root = tkinter.Tk()
#     root.withdraw()
#
#     # maindir = filedialog.askdirectory()
#     shell_base = filedialog.askopenfilename(filetypes=[('.sh', '.sh')])
#     outerdir = os.path.dirname(shell_base)
#
#     maindir = filedialog.askdirectory()
#     prep_individ_run(maindir, shell_base)
