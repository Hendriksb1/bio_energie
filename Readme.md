
# Weather and Energy Price Data ETL Module

This project provides a module to **extract**, **transform**, and **load** (ETL) weather and energy price data from specified APIs. The data is processed and saved in **Parquet format** for efficient storage and retrieval.

## Project Overview

The **Weather and Energy Price Data ETL Module** is designed to gather **real-time weather and energy price data** to support **data-driven decision-making** in various applications, such as energy management, forecasting, and research. By leveraging APIs, this module automates the data collection process, transforming raw data into a structured format suitable for analysis. The collected data can be used to assess **correlations between weather conditions and energy prices**, which is essential for optimizing energy consumption and managing costs effectively.

## Features

- **Scheduled data extraction** from weather and energy price APIs.
- **Data transformation**, including timestamp conversion and additional calculations (e.g., sun index).
- Data is stored in **Parquet files** with timestamps for easy identification.
- **Error handling** for API requests and data processing.

## Requirements

- **Python 3.6 or higher**
- Required libraries:
  - `pandas`
  - `pyarrow`
  - `requests`
  - `python-dotenv`

Install the required libraries using pip:

```bash
pip install pandas pyarrow requests python-dotenv
```

## Installation

1. Clone the repository:

   ```bash
   git clone <repository_url>
   ```

2. Navigate to the project directory:

   ```bash
   cd <project_directory>
   ```

3. Install the dependencies as mentioned above.

4. Create a `.env` file in the project root directory and add your API endpoints:

   ```plaintext
   URL_PRICE=<your_price_api_url>
   URL_WEATHER=<your_weather_api_url>
   ```

## Usage

To run the ETL module, execute the main function in the `main.py` file:

```bash
python main.py
```

The module will continuously **extract** and **transform** the data every **5 seconds**. The processed data will be saved in **Parquet format** in the same directory with **timestamps** in their filenames.

## Contributing

Contributions are welcome! If you have suggestions for improvements or new features, feel free to **open an issue** or **submit a pull request**.

## License

This project is licensed under the **MIT License** – see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Pandas Documentation](https://pandas.pydata.org/)
- [PyArrow Documentation](https://arrow.apache.org/docs/python/)
- [Requests Documentation](https://docs.python-requests.org/en/latest/)
- [Python Dotenv Documentation](https://pypi.org/project/python-dotenv/)
