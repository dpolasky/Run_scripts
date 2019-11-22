"""
Module for editing parameter files by activation type - e.g. creating multiple parameter sets from a given base
for various activation types
"""

import tkinter
from tkinter import filedialog
import os


# Fragger params data - enzyme name, cut after, cut not before
ENZYME_DATA = {'TRYP': ['Trypsin', 'KR', 'P'],
               'CHYTR': ['Chymotrypsin', 'FLWY', 'P'],
               'TRYP+CHYTR': ['Trypsin/Chymotrypsin', 'KRFLWY', 'P']
               }


def edit_param_value(line, new_value):
    """
    Edit a single line of a Fragger parameters file ('=' delimited)
    :param line: line to edit (full string)
    :param new_value: new value to include
    :return: edited line
    """
    splits = line.split('=')
    return '{}= {}\n'.format(splits[0], new_value)


def create_param_file(base_param_file, output_dir, activation_type=None, enzyme=None):
    """
    Create a new param file based on the base param file, edited for the provided activation type
    :param base_param_file:
    :param activation_type: optional
    :param output_dir
    :param enzyme: optional
    :return: void
    """
    # check activation types
    if activation_type is not None:
        if activation_type not in ['HCD', 'CID', 'AIETD', 'ETD', 'EThcD']:
            print('ERROR: ACTIVATION TYPE {} NOT DEFINED'.format(activation_type))
            return

    if enzyme is not None:
        if enzyme not in ['TRYP', 'CHYTR', 'TRYP+CHYTR']:
            print('ERROR: ENZYME {} NOT DEFINED, must be one of: TRYP, CHYTR, TRYP+CHYTR'.format(enzyme))
            return

    output_lines = []
    with open(base_param_file, 'r') as infile:
        for line in list(infile):
            newline = None
            if line.startswith('remove_precursor_peak'):
                if activation_type is not None:
                    if activation_type in ['HCD', 'CID']:
                        newline = edit_param_value(line, 1)
                    else:
                        newline = edit_param_value(line, 2)
            elif line.startswith('fragment_ion_series'):
                if activation_type is not None:
                    glyco = False
                    if 'Y' in line:
                        glyco = True
                    if activation_type in ['HCD', 'CID']:
                        if glyco:
                            if 'b~' in line or 'y~' in line:
                                newline = edit_param_value(line, 'b,y,Y,b~,y~')
                            else:
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
                if activation_type is not None:
                    if activation_type in ['HCD', 'CID', 'EThcD']:
                        newline = edit_param_value(line, 0.1)
                    elif activation_type in ['ETD', 'AIETD']:
                        newline = edit_param_value(line, 0)
                    else:
                        newline = edit_param_value(line, 0)
            elif line.startswith('labile_mod_no_shifted_by_ions'):
                if activation_type is not None:
                    # disable labile mod removing b/y ions if in ETD mode, since b/y ions will not be included in the ion series
                    if activation_type in ['ETD']:
                        newline = edit_param_value(line, 0)
                    else:
                        # don't enable by default, as we don't always want to use this for HCD/etc searches. (Need to remember to set/unset as needed in base param file)
                        newline = line

            # enzyme params
            elif line.startswith('search_enzyme_name'):
                if enzyme is not None:
                    newline = edit_param_value(line, ENZYME_DATA[enzyme][0])
            elif line.startswith('search_enzyme_cutafter'):
                if enzyme is not None:
                    newline = edit_param_value(line, ENZYME_DATA[enzyme][1])
            elif line.startswith('search_enzyme_butnotafter'):
                if enzyme is not None:
                    newline = edit_param_value(line, ENZYME_DATA[enzyme][2])
            elif line.startswith('database_name'):
                if enzyme is not None:
                    newline = 'database_name = ../{}\n'.format(line.split('=')[1].strip())
            else:
                newline = line

            if newline is None:
                newline = line
            output_lines.append(newline)

    # save updated params to new file with activation appended
    if activation_type is not None:
        old_filename = os.path.splitext(os.path.basename(base_param_file))[0]
        new_filename = '{}_{}{}'.format(old_filename, activation_type, os.path.splitext(base_param_file)[1])
        new_path = os.path.join(output_dir, new_filename)
    else:
        # save in folder
        new_path = os.path.join(output_dir, os.path.basename(base_param_file))
    with open(new_path, 'w') as newfile:
        for line in output_lines:
            newfile.write(line)
    return new_path


# def main(activation_types):
#     """
#
#     :param activation_types: list str
#     :return:
#     """
#     param_files = filedialog.askopenfilenames(filetypes=[('.params', '.params')])
#     main_dir = os.path.dirname(param_files[0])
#
#     for param_file in param_files:
#         for activation_type in activation_types:
#             create_param_file(param_file, activation_type, output_dir=main_dir)


def rename_enzyme_mzmls(enzyme_name, file_list):
    """
    Rename mzml files by enzyme - convention is (raw name)_enzyme_activation.mzml
    :param enzyme_name: string to insert in name
    :param file_list: list of mzml file paths
    :return: void
    """
    print('remember, files MUST be sorted by activation first (if doing so)')
    for file in file_list:
        old_filename = os.path.splitext(os.path.basename(file))[0]
        splits = old_filename.split('_')
        splits.insert(len(splits) - 1, enzyme_name)
        new_filename = '_'.join(splits)
        output_path = os.path.join(os.path.dirname(file), new_filename + os.path.splitext(file)[1])
        os.rename(file, output_path)


if __name__ == '__main__':
    root = tkinter.Tk()
    root.withdraw()

    # main(activation_types=['HCD', 'AIETD'])
    files = filedialog.askopenfilenames(filetypes=[('mzml', '.mzml')])
    rename_enzyme_mzmls(enzyme_name='TRYP+CHYTR', file_list=files)
