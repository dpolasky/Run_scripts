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
               'TRYP+CHYTR': ['Trypsin/Chymotrypsin', 'KRFLWY', 'P'],
               'Tryp': ['Trypsin', 'KR', 'P'],
               'LysC': ['lysc', 'K', 'P'],
               'ALP': ['']
               }
# DEPRECATE_DICT = {
#     'offset_rule_mode': 'labile_search_mode',
#     'glyco_search_mode': 'labile_search_mode',
#     # 'Y_type_masses': '',
#     # 'diagnostic_fragments': '',
#     # 'diagnostic_fragments_filter': 'oxonium_intensity_filter',
#     'oxonium_intensity_filter': 'diagnostic_fragments_filter'
#
# }

DEPRECATED_PARAMS = {
    'offset_rule_mode': 'labile_search_mode',
    'glyco_search_mode': 'labile_search_mode',
    'oxonium_ions': 'diagnostic_fragments',
    'diagnostic_fragments_filter': 'diagnostic_intensity_filter',
    'oxonium_intensity_filter': 'diagnostic_intensity_filter',
    'deltamass_allowed_residues': 'restrict_deltamass_to'
}
DEPRECATED_VALUES = {
    'NGlycan': 'nglycan',
    'OGlycan': 'labile',
    'OGlycan ST': 'labile',
    'Specific': 'labile',
    'None': 'off'
}


def deprecated_param_check(line, deprecated_key_dict, deprecated_value_dict):
    """
    Check for lines that have old/deprecated parameters and fix them using the known dict.
    NOTE: saves the old line too, because older test jars may still need it
    :param line: string (line) of param file to consider.
    :type line: str
    :param deprecated_key_dict: dict of deprecated param: new param
    :type deprecated_key_dict: dict
    :param deprecated_value_dict: dict of deprecated value: new value
    :type deprecated_value_dict: dict
    :return: list of lines (old line, new line)
    :rtype: list
    """
    # ignore comments
    if line.startswith('#'):
        return [line]

    splits = line.split('=')
    old_key = splits[0].strip()
    return_lines = []
    try:
        old_value = splits[1].strip()
        if '#' in old_value:
            old_value = old_value.split('#')[0].strip()     # ignore comments when finding values
    except IndexError:
        # retain blank lines and text explanation lines (no params to edit)
        return [line]
    if old_key in deprecated_key_dict:
        # this parameter name has been deprecated. Replace it with the new version
        if old_value in deprecated_value_dict:
            newline = '{} = {}\n'.format(deprecated_key_dict[old_key], deprecated_value_dict[old_value])
            return_lines.append(line)
            return_lines.append(newline)
        else:
            # retain original value
            newline = '{} ={}'.format(deprecated_key_dict[old_key], splits[1])     # splits[1] still has \n at the end
            return_lines.append(line)
            return_lines.append(newline)
    else:
        if old_value in deprecated_value_dict:
            newline = '{}= {}\n'.format(splits[0], deprecated_value_dict[old_value])
            return_lines.append(line)
            return_lines.append(newline)
        else:
            # no change
            return_lines.append(line)

    return return_lines


# def deprecated_param_check(line, deprecation_dict):
#     """
#     Check for lines that have old/deprecated parameters and fix them using the known dict
#     :param line: param file line to read
#     :type line: str
#     :param deprecation_dict: dict of old line: new line to use for fixing
#     :type deprecation_dict: dict
#     :return: updated line
#     :rtype: str
#     """
#     splits = line.split('=')
#     old_key = splits[0].strip()
#     if old_key in deprecation_dict.keys():
#         # this parameter name has been deprecated. Replace it with the new version
#         newline = '{} ={}'.format(deprecation_dict[old_key], splits[1])     # splits[1] still has \n at the end
#         return newline
#     else:
#         return line


def edit_param_value(line, new_value):
    """
    Edit a single line of a Fragger parameters file ('=' delimited)
    :param line: line to edit (full string)
    :param new_value: new value to include
    :return: edited line
    """
    splits = line.split('=')
    return '{}= {}\n'.format(splits[0], new_value)


def create_param_file(base_param_file, output_dir, activation_type=None, enzyme=None, remove_localize_delta_mass=False):
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
        if enzyme not in ENZYME_DATA.keys():
            print('ERROR: ENZYME {} NOT DEFINED, must be in the enzyme data dict'.format(enzyme))
            return

    output_lines = []
    with open(base_param_file, 'r') as infile:
        for unchecked_line in list(infile):
            # check/replace deprecated params
            checked_lines = deprecated_param_check(unchecked_line, DEPRECATED_PARAMS, DEPRECATED_VALUES)
            for line in checked_lines:
                newline = None
                if line.startswith('remove_precursor_peak'):
                    if activation_type is not None:
                        if activation_type in ['HCD', 'CID']:
                            newline = edit_param_value(line, 1)
                        else:
                            newline = edit_param_value(line, 2)
                elif line.startswith('fragment_ion_series'):
                    if activation_type is not None:
                        check_line = line.split('#')[0]     # ignore comments
                        current_series = check_line.split('=')[1].strip().split(',')
                        if activation_type in ['HCD', 'CID']:
                            remove_ions = ['c', 'z']
                        elif activation_type in ['ETD']:
                            remove_ions = ['b', 'y', 'b~', 'y~', 'Y', 'b-18', 'y-18', 'a']
                        elif activation_type in ['AIETD', 'EThcD', 'AI-ETD']:
                            remove_ions = ['b~', 'y~']
                        # removed improper ions then edit the line
                        for remove_type in remove_ions:
                            if remove_type in current_series:
                                current_series.remove(remove_type)

                        newline = edit_param_value(line, ','.join(current_series))

                # elif line.startswith('diagnostic_fragments_filter'):
                elif line.startswith('oxonium_intensity_filter'):
                    if activation_type is not None:
                        # don't add filtering if it has been turned off
                        check_line = line.split('#')[0]
                        if float(check_line.strip().split('=')[1]) == 0:
                            newline = line
                        else:
                            if activation_type in ['HCD', 'CID']:
                                newline = edit_param_value(line, 0.1)
                            elif activation_type in ['EThcD', 'AIETD']:
                                newline = line      # leave hybrid modes at value set in param file
                            elif activation_type in ['ETD']:
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
                elif line.startswith('localize_delta_mass'):
                    if remove_localize_delta_mass:
                        # disable shifted ions for CID/HCD glyco searches
                        if activation_type is not None:
                            if activation_type in ['CID', 'HCD']:
                                newline = edit_param_value(line, 0)

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
