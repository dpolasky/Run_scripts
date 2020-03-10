"""
Quick script to change all the file names in my Fragger (or etc) source to quickly reset capitalization errors
in the IDE.
"""

import os
import tkinter
from tkinter import messagebox


EDIT_DIR = r"C:\Users\dpolasky\IdeaProjects\msfragger\src\edu\umich\andykong\msfragger"

if __name__ == '__main__':
    root = tkinter.Tk()
    root.withdraw()

    # rename files (add 'a' to the start)
    files = [os.path.join(EDIT_DIR, x) for x in os.listdir(EDIT_DIR) if x.endswith('.java')]
    for file in files:
        new_filename = 'a' + os.path.basename(file)
        new_filepath = os.path.join(EDIT_DIR, new_filename)
        os.rename(file, new_filepath)

    # prompt user to click stuff in the IDE
    messagebox.showinfo('Click the IDE!', 'Click on the files in the IDE, then close this prompt (files will be renamed back to originals')

    # fix the filenames (remove 'a' form the start)
    edited_files = [os.path.join(EDIT_DIR, x) for x in os.listdir(EDIT_DIR) if x.endswith('.java')]
    for file in edited_files:
        new_filename = os.path.basename(file)[1:]
        new_filepath = os.path.join(EDIT_DIR, new_filename)
        os.rename(file, new_filepath)
