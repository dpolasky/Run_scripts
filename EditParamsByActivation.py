"""
Module for editing parameter files by activation type - e.g. creating multiple parameter sets from a given base
for various activation types
"""

import tkinter
from tkinter import filedialog
import os


def edit_param_value(line, new_value):
    """
    Edit a single line of a Fragger parameters file ('=' delimited)
    :param line: line to edit (full string)
    :param new_value: new value to include
    :return: edited line
    """
    splits = line.split('=')
    return '{}= {}\n'.format(splits[0], new_value)


def create_param_file(base_param_file, activation_type, output_dir):
    """
    Create a new param file based on the base param file, edited for the provided activation type
    :param base_param_file:
    :param activation_type:
    :param output_dir
    :return: void
    """
    # check activation types
    if activation_type not in ['HCD', 'CID', 'AIETD', 'ETD', 'EThcD']:
        print('ERROR: ACTIVATION TYPE {} NOT DEFINED')
        return

    output_lines = []
    with open(base_param_file, 'r') as infile:
        for line in list(infile):
            if line.startswith('remove_precursor_peak'):
                if activation_type in ['HCD', 'CID']:
                    newline = edit_param_value(line, 1)
                else:
                    newline = edit_param_value(line, 2)
            elif line.startswith('fragment_ion_series'):
                glyco = False
                if 'Y' in line:
                    glyco = True
                if activation_type in ['HCD', 'CID']:
                    if glyco:
                        newline = edit_param_value(line, 'b,y,Y')
                    else:
                        newline = edit_param_value(line, 'b,y')
                elif activation_type in ['ETD']:
                    newline = edit_param_value(line, 'c,z')
                elif activation_type in ['AIETD', 'EThcD']:
                    if glyco:
                        newline = edit_param_value(line, 'b,y,c,z,Y')
                    else:
                        newline = edit_param_value(line, 'b,y,c,z')
            elif line.startswith('oxonium_intensity_filter'):
                if activation_type in ['HCD', 'CID', 'EThcD']:
                    newline = edit_param_value(line, 0.1)
                elif activation_type in ['ETD', 'AIETD']:
                    newline = edit_param_value(line, 0)
                else:
                    newline = edit_param_value(line, 0)
            elif line.startswith('labile_mod_no_shifted_by_ions'):
                # disable labile mod removing b/y ions if in ETD mode, since b/y ions will not be included in the ion series
                if activation_type in ['ETD']:
                    newline = edit_param_value(line, 0)
                else:
                    # don't enable by default, as we don't always want to use this for HCD/etc searches. (Need to remember to set/unset as needed in base param file)
                    newline = line
            else:
                newline = line
            output_lines.append(newline)

    # save updated params to new file with activation appended
    old_filename = os.path.splitext(os.path.basename(base_param_file))[0]
    new_filename = '{}_{}{}'.format(old_filename, activation_type, os.path.splitext(base_param_file)[1])
    new_path = os.path.join(output_dir, new_filename)
    with open(new_path, 'w') as newfile:
        for line in output_lines:
            newfile.write(line)
    return new_path


def main(activation_types):
    """

    :param activation_types: list str
    :return:
    """
    param_files = filedialog.askopenfilenames(filetypes=[('.params', '.params')])
    main_dir = os.path.dirname(param_files[0])

    for param_file in param_files:
        for activation_type in activation_types:
            create_param_file(param_file, activation_type, output_dir=main_dir)


if __name__ == '__main__':
    root = tkinter.Tk()
    root.withdraw()

    main(activation_types=['HCD', 'AIETD'])
