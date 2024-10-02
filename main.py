import os

from src.process_runner import run_data


def main(data_source_folder, results_target_folder):
    if data_source_folder is None:
        data_source_folder = os.path.join(os.getcwd(), 'sample_cities')
    if results_target_folder is None:
        results_target_folder = os.path.join(os.getcwd(), 'test', 'resources')
    run_data(data_source_folder, results_target_folder)

if __name__ == "__main__":
    main()