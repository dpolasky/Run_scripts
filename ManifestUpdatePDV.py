"""
copy the original and update the manifest file for a FragPipe folder to Windows paths.
Intended for viewing results in FP-PDV from a local Windows machine after running the analysis on the server.
"""
import shutil
import pathlib
import os
from Fragpipe_Batch_Runner import update_manifest_windows

path = r"Z:\dpolasky\projects\Glyco\OPair_comparison\Nielsen_skin-TMT-oglyc\__FraggerResults\2025-01-21_semi-STAG-sc12x7_opair-deltas"


def main(fragpipe_folder_path):
    """

    :param fragpipe_folder_path:
    :return:
    """
    manifest_path = pathlib.Path(fragpipe_folder_path) / "fragpipe-files.fp-manifest"
    if not os.path.exists(manifest_path):
        print('Error: could not find manifest file {}'.format(manifest_path))
        exit(1)

    # save a copy
    copy_path = pathlib.Path(fragpipe_folder_path) / "fragpipe-files_copy.fp-manifest"
    if not os.path.exists(copy_path):
        shutil.copy(manifest_path, copy_path)

    # edit manifest
    update_manifest_windows(str(manifest_path))


if __name__ == '__main__':
    main(path)
