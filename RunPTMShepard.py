"""
module to run PTMShepherd and rename/move output files for convenience:
    - choose PSM file(s) to evaluate
    - edit config file with appropriate file paths (raw hard-coded)
    - run PTM shep (using local machine...or could write shell to run manually on server)
    - rename output files to match PSM filename
"""

import tkinter
from tkinter import filedialog
import os
import subprocess

TOOL_PATH = r"\\corexfs.med.umich.edu\proteomics\dpolasky\tools\PTMShep_jar\ptmshepherd-0.2.16.jar"
# RAW_DIR = r"\\corexfs.med.umich.edu\proteomics\dpolasky\data\glyco\OGLYC_PXD009476\Kidney_all"
# RAW_DIR = r"\\corexfs.med.umich.edu\proteomics\dpolasky\data\glyco\OGLYC_PXD009476\Serum"

# RAW_DIR = r"\\corexfs.med.umich.edu\proteomics\dpolasky\data\glyco\pGlyco2\brain_PXD005411"
# RAW_DIR = r"\\corexfs.med.umich.edu\proteomics\dpolasky\data\glyco\pGlyco2\heart_PXD005413"
# RAW_DIR = r"\\corexfs.med.umich.edu\proteomics\dpolasky\data\glyco\pGlyco2\kidney_PXD005412"
# RAW_DIR = r"\\corexfs.med.umich.edu\proteomics\dpolasky\data\glyco\pGlyco2\liver_PXD005553"
# RAW_DIR = r"\\corexfs.med.umich.edu\proteomics\dpolasky\data\glyco\pGlyco2\lung_PXD005555"

RAW_DIR = r"\\corexfs.med.umich.edu\proteomics\dpolasky\data\glyco\2017_01_19_MB1-4_Glyco"


def main_ptmshep(psm_files):
    """
    run PTM Shepherd (see module description)
    :param psm_files: list of files to evaluate
    :type psm_files: list
    :return: void
    :rtype:
    """
    main_dir = os.path.dirname(psm_files[0])
    shepherd_dir = os.path.join(main_dir, '_PTMShepherd')
    if not os.path.exists(shepherd_dir):
        os.makedirs(shepherd_dir)

    # detect primary config file
    config_files = find_specific_files(shepherd_dir, endswith='_config.txt')
    if not len(config_files) == 1:
        config_file = filedialog.askopenfilename(initialdir=shepherd_dir, filetypes=[('config', '_config.txt')])
    else:
        config_file = config_files[0]

    # run PTMShepherd on each PSM file
    for psm_file in psm_files:
        # edit config file
        psm_name = edit_config(config_file, psm_file, RAW_DIR)
        output_dir = os.path.join(shepherd_dir, psm_name)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        os.chdir(output_dir)

        # run PTM-Shepherd
        cmd = 'java -jar {} {}'.format(TOOL_PATH, config_file)
        subprocess.run(cmd)

        # rename outputs
        output_tsvs = [os.path.join(output_dir, x) for x in os.listdir(output_dir) if x.lower().endswith('tsv')]
        for tsv in output_tsvs:
            new_name = os.path.join(output_dir, psm_name + os.path.basename(tsv))
            os.rename(tsv, new_name)


def edit_config(config_path, psm_path, raw_path):
    """
    Edit the PTMShepherd config file to point to the correct files and compute the PSM file short name
    :param config_path: path to config file
    :type config_path: str
    :param psm_path: path to PSM file
    :type psm_path: str
    :param raw_path: path to raw directory
    :type raw_path: str
    :return: PSM file short name
    :rtype: str
    """
    # compute short PSM name
    psm_name = os.path.basename(os.path.splitext(psm_path)[0])

    output_lines = []
    with open(config_path, 'r') as readfile:
        for line in readfile:
            if line.startswith('dataset'):
                newline = 'dataset = {} {} {}\n'.format(psm_name, psm_path, raw_path)
            else:
                newline = line
            output_lines.append(newline)

    with open(config_path, 'w') as outfile:
        for line in output_lines:
            outfile.write(line)
    return psm_name


def find_specific_files(starting_dir, endswith):
    """
    From starting dir, keep jumping up directories until finding file(s) ending with specified string. Returns those
    files
    :param starting_dir: dir to start
    :param endswith: file type to find
    :return: list of filepaths
    """
    files = [os.path.join(starting_dir, x) for x in os.listdir(starting_dir) if x.lower().endswith(endswith.lower())]
    jumps = 0
    while len(files) == 0:
        # jump up second directory if none found
        starting_dir = os.path.dirname(starting_dir)
        files = [os.path.join(starting_dir, x) for x in os.listdir(starting_dir) if x.lower().endswith(endswith.lower())]
        jumps += 1
        if jumps > 5:
            print('ERROR: no files of type {} found!'.format(endswith))
            break
    return files


if __name__ == '__main__':
    root = tkinter.Tk()
    root.withdraw()

    psmfiles = filedialog.askopenfilenames(filetypes=[('PSM file', '_psm.tsv'), ('PSM file', '_byonic.csv'), ('PSM file', '_psm.txt')])
    main_ptmshep(psmfiles)
