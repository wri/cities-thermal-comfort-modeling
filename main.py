import os

from src.process_runner import run_data


def main(data_source_folder, results_target_folder):
    run_data(data_source_folder, results_target_folder)
    b = 2

if __name__ == "__main__":
    main()