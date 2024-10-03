import os

from src.process_runner import run_plugins


def main(data_source_folder, results_target_folder):
    run_plugins(data_source_folder, results_target_folder)

if __name__ == "__main__":
    main()