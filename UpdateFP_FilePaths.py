"""
copy the original and update various files/paths for a FragPipe folder to Windows paths.
Intended for viewing results in FP-PDV or running tools from a local Windows machine after running the analysis on the server.
"""
import shutil
import pathlib
import os
from Fragpipe_Batch_Runner import update_manifest_windows, update_folder_windows

# path = r"Z:\dpolasky\projects\Glyco\OPair_comparison\Nielsen_skin-TMT-oglyc\__FraggerResults\2025-01-21_semi-STAG-sc12x7_opair-deltas"
# path = r"Z:\crojaram\FPOP_Project\FPOP_DIA"
# path = r"Z:\dpolasky\projects\Glyco\pGlyco2\__FraggerResults\2025-02-12_1670-nh4-fe-na-mouse5"
# path = r"Z:\dpolasky\projects\_BuildTests\_results\2025-05-01_23.0-build42\Open"
path = r"Z:\crojaram\ROSA_MULTIPLEXFOOTPRINTING\2025_May\2_2th_SamplePTMP-68only"


def update_shepherd_config(fragpipe_folder_path):
    """

    :param fragpipe_folder_path:
    :return:
    """
    file_path = pathlib.Path(fragpipe_folder_path) / "shepherd.config"
    if not os.path.exists(file_path):
        print('Error: could not find shepherd.config file {}'.format(file_path))
        return

    # save a copy
    copy_path = pathlib.Path(fragpipe_folder_path) / "shepherd_copy.config"
    if not os.path.exists(copy_path):
        shutil.copy(file_path, copy_path)

    # update paths
    with open(file_path, 'r+') as f:
        lines = f.readlines()
        for i, line in enumerate(lines):
            if 'database =' in line:
                lines[i] = update_folder_windows(line)
            elif 'dataset =' in line:
                lines[i] = update_folder_windows(line)
        f.seek(0)
        f.writelines(lines)
        f.truncate()


def update_manifest(fragpipe_folder_path):
    """

    :param fragpipe_folder_path:
    :return:
    """
    file_path = pathlib.Path(fragpipe_folder_path) / "fragpipe-files.fp-manifest"
    if not os.path.exists(file_path):
        print('Error: could not find manifest file {}'.format(file_path))
        exit(1)

    # save a copy
    copy_path = pathlib.Path(fragpipe_folder_path) / "fragpipe-files_copy.fp-manifest"
    if not os.path.exists(copy_path):
        shutil.copy(file_path, copy_path)

    # edit manifest
    update_manifest_windows(str(file_path))


def main(fragpipe_folder_path):
    """
    update the paths in the manifest and shepherd config files
    :param fragpipe_folder_path:
    :return:
    """
    update_manifest(fragpipe_folder_path)
    update_shepherd_config(fragpipe_folder_path)


if __name__ == '__main__':
    main(path)
