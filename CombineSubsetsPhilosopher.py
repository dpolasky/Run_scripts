"""
Module for taking different subsets of data (e.g. activation type) run separately in Fragger for combined
analysis by Philosopher. Creates a new folder for the Philosopher analysis, soft links pepXML and tsv files,
and generates a shell script to run philosopher
"""

import tkinter
from tkinter import filedialog
import os
import CombinePSMs

ACTIVATION_TYPES = ['HCD', 'ETD']
OTHER_INFO = ['2000', '2500', '4000']
# ACTIVATION_TYPES = ['HCD', 'EThcD']


def link_file(input_file, current_activation, analysis_combined_dir):
    """
    helper method to create symlink from old directory to new combined one
    :param input_file:
    :param current_activation:
    :param analysis_combined_dir:
    :return:
    """
    new_filename = os.path.splitext(os.path.basename(input_file))[0]
    extension = os.path.splitext(input_file)[1]
    new_filename = '{}_{}{}'.format(new_filename, current_activation, extension)
    new_filepath = os.path.join(analysis_combined_dir, new_filename)
    os.link(input_file, new_filepath)


def main():
    maindir = filedialog.askdirectory()
    results_folders = [os.path.join(maindir, x) for x in os.listdir(maindir)]

    # combine_dict = CombinePSMs.sort_by_combinetype(analysis_folders, ACTIVATION_TYPES, ignore_date=False)
    combine_dir = os.path.join(maindir, os.path.join('__FraggerResults', '_Combined_Philsopher'))
    if not os.path.exists(combine_dir):
        os.makedirs(combine_dir)

    # link files and name by activation type
    for results_folder in results_folders:
        # determine activation type
        splits = os.path.splitext(os.path.basename(results_folder))[0].split('_')
        current_activation = None
        for activation_type_string in ACTIVATION_TYPES:
            if activation_type_string in splits:
                current_activation = activation_type_string
        if current_activation is None:
            print('ERROR: no activation method found for file {}'.format(results_folder))

        # link all pepXML and tsv files, appending the activation type
        pepxml_files = [x for x in os.listdir(results_folder) if x.endswith('.pepXML')]
        tsv_files = [x for x in os.listdir(results_folder) if x.endswith('.tsv')]
        for file in pepxml_files:
            link_file(file, current_activation, combine_dir)
        for file in tsv_files:
            link_file(file, current_activation, combine_dir)

    #
    # for analysis_name, analysis_original_folder_list in combine_dict.items():
    #     # create a new combined directory for this analysis
    #     analysis_combined_dir = os.path.join(combine_dir, analysis_name)
    #     if not os.path.exists(analysis_combined_dir):
    #         os.makedirs(analysis_combined_dir)
    #
    #     # link all files to the combined analysis directory *while editing file names by activation so they don't overlap*
    #     for analysis_original_folder in analysis_original_folder_list:
    #         # Get activation method for this subset to rename linked files
    #         current_activation = ''
    #         for activation_type in ACTIVATION_TYPES:
    #             splits = analysis_original_folder.split('_')
    #             if activation_type in splits:
    #                 current_activation = activation_type
    #
    #         # link files
    #         pepxml_files = [x for x in os.listdir(analysis_original_folder) if x.endswith('.pepXML')]
    #         tsv_files = [x for x in os.listdir(analysis_original_folder) if x.endswith('.tsv')]
    #         for file in pepxml_files:
    #             link_file(file, current_activation, analysis_combined_dir)
    #         for file in tsv_files:
    #             link_file(file, current_activation, analysis_combined_dir)
    #
    #     # generate shell script to run philosopher on combined folder


if __name__ == '__main__':
    root = tkinter.Tk()
    root.withdraw()

    main()

